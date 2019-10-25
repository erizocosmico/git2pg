package git2pg

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/src-d/go-borges"
	"golang.org/x/sync/errgroup"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

type migrator struct {
	lib              borges.Library
	db               *sql.DB
	repoWorkers      int
	allowBinaryBlobs bool
	maxBlobSize      uint64

	repositories *tableCopier
	refs         *tableCopier
	refCommits   *tableCopier
	commits      *tableCopier
	treeEntries  *tableCopier
	treeBlobs    *tableCopier
	files        *tableCopier
	blobs        *tableCopier
	remotes      *tableCopier
}

var tables = []string{
	"repositories",
	"remotes",
	"refs",
	"ref_commits",
	"commits",
	"tree_entries",
	"tree_blobs",
	"tree_files",
	"blobs",
}

// Options to configure the migration of a library to the database.
type Options struct {
	// Workers to use for migration. That means, number of repositories that
	// can be processed at the same time.
	Workers int
	// RepoWorkers is the number of workers to use while processing a single
	// repository. So, workers * repo workers is the total number of workers
	// that can be running at any given time during the migration.
	RepoWorkers int
	// Full migrates all data from all trees in the repository, instead of just
	// the ones referenced in the HEAD commits of each reference.
	Full bool
	// MaxBlobSize in megabytes to import a blob.
	MaxBlobSize uint64
	// AllowBinaryBlobs to be exported.
	AllowBinaryBlobs bool
}

// Migrate the given git repositories library to the given database.
func Migrate(
	ctx context.Context,
	lib borges.Library,
	db *sql.DB,
	opts Options,
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
		lib:              lib,
		db:               db,
		repositories:     copiers["repositories"],
		refs:             copiers["refs"],
		refCommits:       copiers["ref_commits"],
		commits:          copiers["commits"],
		treeEntries:      copiers["tree_entries"],
		treeBlobs:        copiers["tree_blobs"],
		files:            copiers["tree_files"],
		blobs:            copiers["blobs"],
		remotes:          copiers["remotes"],
		repoWorkers:      opts.RepoWorkers,
		allowBinaryBlobs: opts.AllowBinaryBlobs,
		maxBlobSize:      opts.MaxBlobSize,
	}

	iter, err := lib.Repositories(borges.ReadOnlyMode)
	if err != nil {
		return fmt.Errorf("cannot get repositories from library: %s", err)
	}

	var tokens = make(chan struct{}, opts.Workers)
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
			if err := m.migrateRepository(ctx, repo, opts.Full); err != nil {
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
		m.remotes,
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
	full bool,
) error {
	if err := m.repositories.copy(r.ID()); err != nil {
		return fmt.Errorf("unable to copy repository: %s", err)
	}

	log := logrus.WithField("repo", r.ID())
	start := time.Now()
	if err := m.migrateRemotes(ctx, r); err != nil {
		return fmt.Errorf("cannot migrate remotes: %s", err)
	}
	log.WithField("elapsed", time.Since(start)).Debug("migrated remotes")

	start = time.Now()
	trees, err := m.migrateRefs(ctx, r, full)
	if err != nil {
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

func (m *migrator) migrateRemotes(
	ctx context.Context,
	repo borges.Repository,
) error {
	conf, err := repo.R().Config()
	if err != nil {
		return fmt.Errorf("unable to get repository config: %s", err)
	}

	for _, r := range conf.Remotes {
		var refspecs = make([]string, len(r.Fetch))
		for i, r := range r.Fetch {
			refspecs[i] = string(r)
		}

		err := m.remotes.copy(
			repo.ID(),
			r.Name,
			pq.Array(r.URLs),
			pq.Array(refspecs),
		)
		if err != nil {
			return fmt.Errorf("unable to copy remote: %s", err)
		}
	}

	return nil
}

func (m *migrator) migrateRefs(
	ctx context.Context,
	repo borges.Repository,
	full bool,
) (rootTrees []plumbing.Hash, err error) {
	var (
		treesMut   sync.Mutex
		g          errgroup.Group
		tokens     = make(chan struct{}, m.repoWorkers)
		graphCache = newGraphCache()
		r          = newSyncRepository(repo)
		iter       = &refIterator{repo: r}
	)

	defer iter.close()

	for {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		default:
		}

		ref, commit, err := iter.next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("cannot get next reference: %s", err)
		}

		tokens <- struct{}{}

		start := time.Now()
		g.Go(func() error {
			defer func() {
				<-tokens
				logrus.WithFields(logrus.Fields{
					"elapsed": time.Since(start),
					"ref":     ref.Name().String(),
					"repo":    r.ID(),
				}).Debug("migrated reference")
			}()

			err := m.refs.copy(
				repo.ID(),
				ref.Name().String(),
				commit.Hash.String(),
			)
			if err != nil {
				return err
			}

			listener := &refCommitsListener{
				repo:       r,
				refHash:    commit.Hash,
				full:       full,
				refCommits: m.refCommits,
				commits:    m.commits,
				onCommitTreeSeen: func(h plumbing.Hash) {
					treesMut.Lock()
					rootTrees = append(rootTrees, h)
					treesMut.Unlock()
				},
			}

			return visitRefCommits(
				ctx,
				r,
				ref,
				commit,
				listener,
				graphCache,
			)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return rootTrees, nil
}

func (m *migrator) migrateTrees(
	ctx context.Context,
	r borges.Repository,
	trees []plumbing.Hash,
) error {
	listener := &fileTreeListener{
		repo:             r,
		treeEntries:      m.treeEntries,
		treeBlobs:        m.treeBlobs,
		files:            m.files,
		blobs:            m.blobs,
		maxBlobSize:      m.maxBlobSize,
		allowBinaryBlobs: m.allowBinaryBlobs,
	}

	treesCache := newTreesCache()
	tokens := make(chan struct{}, m.repoWorkers)
	repo := newSyncRepository(r)
	var g errgroup.Group

	start := time.Now()
	for _, tree := range trees {
		tree := tree
		tokens <- struct{}{}
		g.Go(func() error {
			start := time.Now()
			defer func() {
				<-tokens
				logrus.WithFields(logrus.Fields{
					"elapsed": time.Since(start),
					"tree":    tree.String(),
					"repo":    r.ID(),
				}).Debug("migrated tree")
			}()

			return visitFileTree(ctx, repo, tree, listener, treesCache)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logrus.WithField("elapsed", time.Since(start)).Debugf("migrated %d trees", len(trees))
	return nil
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
