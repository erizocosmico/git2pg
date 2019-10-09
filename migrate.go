package git2pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/src-d/enry/v2"
	"github.com/src-d/go-borges"
	"golang.org/x/sync/errgroup"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// TODO(erizocosmico): association between root trees and tree entries and
// remotes.

type migrator struct {
	lib         borges.Library
	db          *sql.DB
	repoWorkers int

	repositories *tableCopier
	refs         *tableCopier
	refCommits   *tableCopier
	commits      *tableCopier
	treeEntries  *tableCopier
	treeBlobs    *tableCopier
	files        *tableCopier
	blobs        *tableCopier
}

var tables = []string{
	"repositories",
	"refs",
	"ref_commits",
	"commits",
	"tree_entries",
	"tree_blobs",
	"tree_files",
	"blobs",
}

// Migrate the given git repositories library to the given database.
func Migrate(
	ctx context.Context,
	lib borges.Library,
	db *sql.DB,
	workers, repoWorkers int,
) error {
	var copiers = make(map[string]*tableCopier)
	for _, t := range tables {
		var err error
		copiers[t], err = newTableCopier(ctx, db, t)
		if err != nil {
			return fmt.Errorf("cannot create copier for %s: %s", t, err)
		}
	}

	m := &migrator{
		lib:          lib,
		db:           db,
		repositories: copiers["repositories"],
		refs:         copiers["refs"],
		refCommits:   copiers["ref_commits"],
		commits:      copiers["commits"],
		treeEntries:  copiers["tree_entries"],
		treeBlobs:    copiers["tree_blobs"],
		files:        copiers["tree_files"],
		blobs:        copiers["blobs"],
		repoWorkers:  repoWorkers,
	}

	iter, err := lib.Repositories(borges.ReadOnlyMode)
	if err != nil {
		return fmt.Errorf("cannot get repositories from library: %s", err)
	}

	var tokens = make(chan struct{}, workers)
	var g errgroup.Group
	var repos int
	start := time.Now()
	for {
		tokens <- struct{}{}
		repo, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("unable to get next repository: %s", err)
		}

		repos++

		log := logrus.WithField("repo", repo.ID())

		g.Go(func() error {
			start := time.Now()
			defer func() {
				<-tokens
			}()

			log.Debug("migrating repository")
			if err := m.migrateRepository(ctx, repo); err != nil {
				return err
			}
			log.WithField("elapsed", time.Since(start)).Info("migrated repository")

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"repos":   repos,
		"elapsed": time.Since(start),
	}).Debug("migrated all repositories")

	return m.flush()
}

func (m *migrator) flush() error {
	var copiers = []*tableCopier{
		m.repositories,
		m.refs,
		m.refCommits,
		m.commits,
		m.treeEntries,
		m.treeBlobs,
		m.files,
		m.blobs,
	}

	for _, copier := range copiers {
		if err := copier.Close(); err != nil {
			return fmt.Errorf("could not close copier correctly: %s", err)
		}
	}

	return nil
}

func (m *migrator) migrateRepository(
	ctx context.Context,
	r borges.Repository,
) error {
	if err := m.repositories.copy(r.ID()); err != nil {
		return fmt.Errorf("unable to copy repository: %s", err)
	}

	var trees []plumbing.Hash
	onCommitSeen := func(c *object.Commit) {
		trees = append(trees, c.TreeHash)
	}

	log := logrus.WithField("repo", r.ID())
	start := time.Now()
	if err := m.migrateRefs(ctx, r, onCommitSeen); err != nil {
		return fmt.Errorf("cannot migrate refs: %s", err)
	}
	log.WithField("elapsed", time.Since(start)).Debug("migrated references and commits")

	start = time.Now()
	if err := m.migrateTrees(ctx, r, dedupHashes(trees)); err != nil {
		return fmt.Errorf("cannot migrate trees: %s", err)
	}
	log.WithField("elapsed", time.Since(start)).Debug("migrated trees and files")

	return r.Close()
}

func (m *migrator) migrateRefs(
	ctx context.Context,
	repo borges.Repository,
	commitSeen func(*object.Commit),
) error {
	graphCache := make(map[plumbing.Hash][]plumbing.Hash)

	onCommitSeen := func(c *object.Commit) error {
		if _, ok := graphCache[c.Hash]; ok {
			return nil
		}

		commitSeen(c)

		parentHashes := make([]string, len(c.ParentHashes))
		for i, h := range c.ParentHashes {
			parentHashes[i] = h.String()
		}

		parents, err := json.Marshal(parentHashes)
		if err != nil {
			return fmt.Errorf("cannot marshal commit parents: %s", err)
		}

		values := []interface{}{
			repo.ID(),
			c.Hash.String(),
			sanitizeString(c.Author.Name),
			sanitizeString(c.Author.Email),
			c.Author.When.Format(time.RFC3339),
			sanitizeString(c.Committer.Name),
			sanitizeString(c.Committer.Email),
			c.Committer.When.Format(time.RFC3339),
			sanitizeString(c.Message),
			c.TreeHash.String(),
			parents,
		}

		if err := m.commits.copy(values...); err != nil {
			return fmt.Errorf("cannot copy commit %s: %s", c.Hash, err)
		}

		return nil
	}

	onRefCommitSeen := func(ref *plumbing.Reference, commit plumbing.Hash, idx int) error {
		return m.refCommits.copy(
			repo.ID(),
			commit.String(),
			ref.Name().String(),
			uint64(idx),
		)
	}

	head, err := repo.R().Head()
	if err != nil && err != plumbing.ErrReferenceNotFound {
		return fmt.Errorf("cannot get HEAD reference: %s", err)
	}

	refs, err := repo.R().References()
	if err != nil {
		return fmt.Errorf("cannot get references: %s", err)
	}
	defer refs.Close()

	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		var ref *plumbing.Reference
		if head == nil {
			ref, err = refs.Next()
			if err != nil {
				if err == io.EOF {
					break
				}

				return fmt.Errorf("cannot get next reference: %s", err)
			}

			if isIgnoredReference(ref) {
				continue
			}
		} else {
			ref = head
			head = nil
		}

		commit, err := resolveCommit(repo.R(), ref.Hash())
		if err != nil {
			if err == errInvalidCommit {
				continue
			}

			return fmt.Errorf("cannot resolve commit: %s", err)
		}

		err = m.refs.copy(
			repo.ID(),
			ref.Name().String(),
			commit.Hash.String(),
		)
		if err != nil {
			return err
		}

		err = visitRefCommits(
			ctx,
			repo,
			ref,
			commit,
			onCommitSeen,
			onRefCommitSeen,
			graphCache,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *migrator) migrateTrees(
	ctx context.Context,
	r borges.Repository,
	trees []plumbing.Hash,
) error {
	onTreeEntrySeen := func(tree plumbing.Hash, entry object.TreeEntry) error {
		values := []interface{}{
			r.ID(),
			entry.Name,
			entry.Hash.String(),
			tree.String(),
			strconv.FormatInt(int64(entry.Mode), 8),
		}
		if err := m.treeEntries.copy(values...); err != nil {
			return fmt.Errorf("cannot copy tree entry: %s", err)
		}

		return nil
	}

	onTreeBlobSeen := func(tree, blob plumbing.Hash) error {
		values := []interface{}{
			r.ID(),
			tree.String(),
			blob.String(),
		}

		if err := m.treeBlobs.copy(values...); err != nil {
			return fmt.Errorf("cannot copy tree blob: %s", err)
		}

		return nil
	}

	onFileSeen := func(path string, tree, blob plumbing.Hash) error {
		values := []interface{}{
			r.ID(),
			tree.String(),
			path,
			blob.String(),
			enry.IsVendor(path),
		}

		if err := m.files.copy(values...); err != nil {
			return fmt.Errorf("cannot copy file: %s", err)
		}

		return nil
	}

	onBlobSeen := func(blob *object.Blob, content []byte) error {
		values := []interface{}{
			r.ID(),
			blob.Hash.String(),
			blob.Size,
			content,
			isBinary(content),
		}

		if err := m.blobs.copy(values...); err != nil {
			return fmt.Errorf("cannot copy tree blob: %s", err)
		}

		return nil
	}

	treesCache := newTreesCache()
	tokens := make(chan struct{}, m.repoWorkers)
	repo := &syncRepository{Repository: r}
	var g errgroup.Group

	for _, tree := range trees {
		tree := tree
		tokens <- struct{}{}
		g.Go(func() error {
			start := time.Now()
			defer func() {
				logrus.WithFields(logrus.Fields{
					"elapsed": time.Since(start),
					"tree":    tree.String(),
					"repo":    r.ID(),
				}).Debug("migrated tree")
				<-tokens
			}()

			return visitFileTree(
				ctx,
				repo,
				tree,
				onTreeEntrySeen,
				onTreeBlobSeen,
				onFileSeen,
				onBlobSeen,
				treesCache,
			)
		})
	}

	return g.Wait()
}

func dedupHashes(hashes []plumbing.Hash) []plumbing.Hash {
	var result []plumbing.Hash
	var seen = make(map[plumbing.Hash]struct{})
	for _, h := range hashes {
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		result = append(result, h)
	}
	return result
}
