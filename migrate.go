package git2pg

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/src-d/enry/v2"
	"github.com/src-d/go-borges"
	"golang.org/x/sync/errgroup"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

// TODO(erizocosmico): association between root trees and tree entries and
// remotes.

type migrator struct {
	lib borges.Library
	db  *sql.DB

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

func Migrate(
	ctx context.Context,
	lib borges.Library,
	db *sql.DB,
	workers int,
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
				log.WithField("elapsed", time.Since(start)).
					Info("migrated repository")
				<-tokens
			}()
			log.Debug("migrating repository")
			return m.migrateRepository(ctx, repo)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"repos":   repos,
		"elapsed": time.Since(start),
	}).Debug("migrated all repositories")
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
	if err := m.repositories.copy(hash(r.ID()), r.ID()); err != nil {
		return fmt.Errorf("unable to copy repository: %s", err)
	}

	log := logrus.WithField("repo", r.ID())
	start := time.Now()
	if err := m.migrateRefs(ctx, r); err != nil {
		return fmt.Errorf("cannot migrate refs: %s", err)
	}
	log.WithField("elapsed", time.Since(start)).Debug("migrated references and commits")

	start = time.Now()
	if err := m.migrateFiles(ctx, r); err != nil {
		return fmt.Errorf("cannot migrate files: %s", err)
	}
	log.WithField("elapsed", time.Since(start)).Debug("migrated trees and files")

	return r.Close()
}

func (m *migrator) migrateRefs(ctx context.Context, repo borges.Repository) error {
	var (
		refs    storer.ReferenceIter
		head    *plumbing.Reference
		commits *indexedCommitIter
		ref     *plumbing.Reference
		err     error
	)

	notifier := copyCommitNotifier(m.commits, repo)

	// TODO(erizocosmico): refactor this as a visitor and move it to its file
	// just like we do with files.
	for {
		if refs == nil {
			refs, err = repo.R().References()
			if err != nil {
				return fmt.Errorf("cannot get references: %s", err)
			}

			head, err = repo.R().Head()
			if err != nil && err != plumbing.ErrReferenceNotFound {
				return fmt.Errorf("cannot get HEAD reference: %s", err)
			}
		}

		if commits == nil {
			var r *plumbing.Reference
			if head == nil {
				r, err = refs.Next()
				if err != nil {
					if err == io.EOF {
						break
					}

					return fmt.Errorf("cannot get next reference: %s", err)
				}
			} else {
				r = plumbing.NewHashReference(plumbing.ReferenceName("HEAD"), head.Hash())
				head = nil
			}

			ref = r
			if isIgnoredReference(ref) {
				continue
			}

			commit, err := resolveCommit(repo.R(), ref.Hash())
			if err != nil {
				if err == errInvalidCommit {
					continue
				}

				return fmt.Errorf("cannot resolve commit: %s", err)
			}

			err = m.refs.copy(
				hash(repo.ID(), ref.Name().String()),
				repo.ID(),
				ref.Name().String(),
				commit.Hash.String(),
			)
			if err != nil {
				return fmt.Errorf("cannot copy reference: %s", err)
			}

			if err := notifier(commit); err != nil {
				return err
			}

			commits = newIndexedCommitIter(repo, commit, notifier)
		}

		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		commit, idx, err := commits.next()
		if err != nil {
			if err == io.EOF {
				commits = nil
				continue
			}

			return fmt.Errorf("cannot get next commit: %s", err)
		}

		hash := hash(repo.ID(), ref.Name().String(), idx)
		values := []interface{}{
			repo.ID(),
			commit.Hash.String(),
			ref.Name().String(),
			int64(idx),
		}

		if err := m.refCommits.copy(hash, values...); err != nil {
			return fmt.Errorf("cannot copy ref commit: %s", err)
		}
	}

	if refs != nil {
		refs.Close()
	}

	return nil
}

func (m *migrator) migrateFiles(ctx context.Context, r borges.Repository) error {
	commits, err := r.R().CommitObjects()
	if err != nil {
		return fmt.Errorf("error getting commits: %s", err)
	}

	rootTrees := make(map[plumbing.Hash]struct{})
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		commit, err := commits.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error getting next commit: %s", err)
		}

		rootTrees[commit.TreeHash] = struct{}{}
	}

	onTreeEntrySeen := func(tree plumbing.Hash, entry object.TreeEntry) error {
		hash := hash(r.ID(), tree, entry.Name)

		values := []interface{}{
			r.ID(),
			entry.Name,
			entry.Hash.String(),
			tree.String(),
			strconv.FormatInt(int64(entry.Mode), 8),
		}
		if err := m.treeEntries.copy(hash, values...); err != nil {
			return fmt.Errorf("cannot copy tree entry: %s", err)
		}

		return nil
	}
	onTreeBlobSeen := func(tree, blob plumbing.Hash) error {
		hash := hash(r.ID(), tree, blob)
		values := []interface{}{
			r.ID(),
			tree.String(),
			blob.String(),
		}

		if err := m.treeBlobs.copy(hash, values...); err != nil {
			return fmt.Errorf("cannot copy tree blob: %s", err)
		}

		return nil
	}
	onFileSeen := func(path string, tree, blob plumbing.Hash) error {
		hash := hash(r.ID(), tree, blob)
		values := []interface{}{
			r.ID(),
			tree.String(),
			path,
			blob.String(),
			enry.IsVendor(path),
		}

		if err := m.files.copy(hash, values...); err != nil {
			return fmt.Errorf("cannot copy file: %s", err)
		}

		return nil
	}
	onBlobSeen := func(blob *object.Blob) error {
		hash := hash(r.ID(), blob.Hash)

		rd, err := blob.Reader()
		if err != nil {
			return fmt.Errorf("cannot get blob reader: %s", err)
		}

		content, err := ioutil.ReadAll(rd)
		if err != nil {
			return fmt.Errorf("cannot read blob content: %s", err)
		}

		values := []interface{}{
			r.ID(),
			blob.Hash.String(),
			blob.Size,
			content,
			isBinary(content),
		}

		if err := m.blobs.copy(hash, values...); err != nil {
			return fmt.Errorf("cannot copy tree blob: %s", err)
		}

		return nil
	}

	seenBlobs := make(map[plumbing.Hash]struct{})
	treeCache := make(map[plumbing.Hash]*treeNode)
	for tree := range rootTrees {
		start := time.Now()
		logrus.WithFields(logrus.Fields{
			"repo": r.ID(),
			"tree": tree.String(),
		}).Debug("migrating tree")
		err := visitFileTree(
			ctx,
			r,
			tree,
			onTreeEntrySeen,
			onTreeBlobSeen,
			onFileSeen,
			onBlobSeen,
			seenBlobs,
			treeCache,
		)
		if err != nil {
			return err
		}

		logrus.WithFields(logrus.Fields{
			"elapsed": time.Since(start),
			"tree":    tree.String(),
			"repo":    r.ID(),
		}).Debug("migrated tree")
	}

	return nil
}
