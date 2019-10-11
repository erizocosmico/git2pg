package git2pg

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/src-d/enry/v2"
	"github.com/src-d/go-borges"
	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

func TestMigrateRepository(t *testing.T) {
	db, cleanDB := setupDB(t)
	defer db.Close()
	defer cleanDB()

	repo, cleanup := setupRepo(t)
	defer cleanup()

	m := setupMigrator(t, db)

	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(m.migrateRepository(ctx, repo, true))
	require.NoError(m.flush())

	assertRepo(t, db, repo)
}

func assertRepo(t *testing.T, db *sql.DB, repo borges.Repository) {
	t.Helper()

	rows := repoTableRows(t, db, "repositories", repo, scanRepo)
	require.Equal(t, [][]interface{}{{string(repo.ID())}}, rows)

	assertRefs(t, db, repo)
	assertCommits(t, db, repo)
	assertTreeEntries(t, db, repo)
	assertBlobs(t, db, repo)
	assertTreeBlobs(t, db, repo)
	assertTreeFiles(t, db, repo)
}

func assertRefs(t *testing.T, db *sql.DB, repo borges.Repository) {
	t.Helper()
	rows := repoTableRows(t, db, "refs", repo, scanRef)

	iter, err := repo.R().References()
	require.NoError(t, err)

	head, err := repo.R().Head()
	require.NoError(t, err)

	expected := [][]interface{}{
		{string(repo.ID()), "HEAD", head.Hash().String()},
	}

	require.NoError(t, iter.ForEach(func(r *plumbing.Reference) error {
		if isIgnoredReference(r) {
			return nil
		}

		expected = append(
			expected,
			[]interface{}{
				string(repo.ID()),
				r.Name().String(),
				r.Hash().String(),
			},
		)
		return nil
	}))

	sortByHash(expected, 2)
	sortByHash(rows, 2)

	require.Equal(t, expected, rows)
}

func assertBlobs(t *testing.T, db *sql.DB, repo borges.Repository) {
	t.Helper()
	rows := repoTableRows(t, db, "blobs", repo, scanBlob)

	iter, err := repo.R().CommitObjects()
	require.NoError(t, err)

	var seen = make(map[plumbing.Hash]struct{})
	var expected [][]interface{}
	require.NoError(t, iter.ForEach(func(c *object.Commit) error {
		fiter, err := c.Files()
		if err != nil {
			return err
		}

		return fiter.ForEach(func(f *object.File) error {
			if _, ok := seen[f.Blob.Hash]; ok {
				return nil
			}

			seen[f.Blob.Hash] = struct{}{}
			rd, err := f.Blob.Reader()
			require.NoError(t, err)

			content, err := ioutil.ReadAll(rd)
			require.NoError(t, err)

			expected = append(
				expected,
				[]interface{}{
					string(repo.ID()),
					f.Blob.Hash.String(),
					f.Blob.Size,
					content,
					isBinary(content),
				},
			)
			return nil
		})
	}))

	require.ElementsMatch(t, expected, rows)
}

type treeBlob struct {
	tree plumbing.Hash
	blob plumbing.Hash
}

func assertTreeBlobs(t *testing.T, db *sql.DB, repo borges.Repository) {
	t.Helper()
	rows := repoTableRows(t, db, "tree_blobs", repo, scanTreeBlob)

	iter, err := repo.R().CommitObjects()
	require.NoError(t, err)

	var seen = make(map[treeBlob]struct{})
	var expected [][]interface{}
	require.NoError(t, iter.ForEach(func(c *object.Commit) error {
		fiter, err := c.Files()
		if err != nil {
			return err
		}

		return fiter.ForEach(func(f *object.File) error {
			tb := treeBlob{c.TreeHash, f.Blob.Hash}
			if _, ok := seen[tb]; ok {
				return nil
			}

			seen[tb] = struct{}{}

			expected = append(
				expected,
				[]interface{}{
					string(repo.ID()),
					c.TreeHash.String(),
					f.Blob.Hash.String(),
				},
			)
			return nil
		})
	}))

	require.ElementsMatch(t, expected, rows)
}

func assertTreeFiles(t *testing.T, db *sql.DB, repo borges.Repository) {
	t.Helper()
	rows := repoTableRows(t, db, "tree_files", repo, scanTreeFile)

	iter, err := repo.R().CommitObjects()
	require.NoError(t, err)

	var expected [][]interface{}
	seen := make(map[string]struct{})
	require.NoError(t, iter.ForEach(func(c *object.Commit) error {
		fiter, err := c.Files()
		if err != nil {
			return err
		}

		return fiter.ForEach(func(f *object.File) error {
			key := c.TreeHash.String() + f.Name
			if _, ok := seen[key]; ok {
				return nil
			}
			seen[key] = struct{}{}

			expected = append(
				expected,
				[]interface{}{
					string(repo.ID()),
					c.TreeHash.String(),
					f.Name,
					f.Blob.Hash.String(),
					enry.IsVendor(f.Name),
				},
			)
			return nil
		})
	}))

	require.ElementsMatch(t, expected, rows)
}

func assertCommits(t *testing.T, db *sql.DB, repo borges.Repository) {
	t.Helper()
	rows := repoTableRows(t, db, "commits", repo, scanCommit)

	iter, err := repo.R().CommitObjects()
	require.NoError(t, err)

	var expected [][]interface{}
	require.NoError(t, iter.ForEach(func(c *object.Commit) error {
		parentHashes := make([]string, len(c.ParentHashes))
		for i, h := range c.ParentHashes {
			parentHashes[i] = h.String()
		}

		expected = append(
			expected,
			[]interface{}{
				string(repo.ID()),
				c.Hash.String(),
				sanitizeString(c.Author.Name),
				sanitizeString(c.Author.Email),
				c.Author.When.UTC().Format(time.RFC3339),
				sanitizeString(c.Committer.Name),
				sanitizeString(c.Committer.Email),
				c.Committer.When.UTC().Format(time.RFC3339),
				sanitizeString(c.Message),
				c.TreeHash.String(),
				parentHashes,
			},
		)
		return nil
	}))

	sort.Slice(expected, func(i, j int) bool {
		return strings.Compare(
			expected[i][1].(string),
			expected[j][1].(string),
		) < 0
	})

	sortByHash(rows, 1)
	sortByHash(expected, 1)

	require.Equal(t, expected, rows)
}

func sortByHash(rows [][]interface{}, hashIdx int) {
	sort.Slice(rows, func(i, j int) bool {
		return strings.Compare(
			rows[i][hashIdx].(string),
			rows[j][hashIdx].(string),
		) < 0
	})
}

func assertTreeEntries(t *testing.T, db *sql.DB, repo borges.Repository) {
	t.Helper()
	rows := repoTableRows(t, db, "tree_entries", repo, scanTreeEntry)

	iter, err := repo.R().TreeObjects()
	require.NoError(t, err)

	var expected [][]interface{}
	require.NoError(t, iter.ForEach(func(t *object.Tree) error {
		for _, entry := range t.Entries {
			expected = append(
				expected,
				[]interface{}{
					string(repo.ID()),
					entry.Name,
					entry.Hash.String(),
					t.Hash.String(),
					strconv.FormatInt(int64(entry.Mode), 8),
				},
			)
		}
		return nil
	}))

	sort.Slice(rows, func(i, j int) bool {
		return strings.Compare(
			rows[i][3].(string)+rows[i][1].(string),
			rows[j][3].(string)+rows[j][1].(string),
		) < 0
	})

	sort.Slice(expected, func(i, j int) bool {
		return strings.Compare(
			expected[i][3].(string)+expected[i][1].(string),
			expected[j][3].(string)+expected[j][1].(string),
		) < 0
	})

	require.Equal(t, expected, rows)
}

func repoTableRows(
	t *testing.T,
	db *sql.DB,
	table string,
	repo borges.Repository,
	scanFunc func(*sql.Rows) ([]interface{}, error),
) [][]interface{} {
	t.Helper()

	rows, err := db.Query(
		fmt.Sprintf("SELECT * FROM %s WHERE repository_id = $1", table),
		string(repo.ID()),
	)
	require.NoError(t, err)

	var result [][]interface{}
	for rows.Next() {
		row, err := scanFunc(rows)
		require.NoError(t, err)
		result = append(result, row)
	}

	return result
}

func scanRepo(rows *sql.Rows) ([]interface{}, error) {
	var id string
	if err := rows.Scan(&id); err != nil {
		return nil, err
	}
	return []interface{}{id}, nil
}

func scanRef(rows *sql.Rows) ([]interface{}, error) {
	var id, name, hash string
	if err := rows.Scan(&id, &name, &hash); err != nil {
		return nil, err
	}
	return []interface{}{id, name, hash}, nil
}

func scanRefCommit(rows *sql.Rows) ([]interface{}, error) {
	var id, name, hash string
	var index int
	if err := rows.Scan(&id, &name, &hash, &index); err != nil {
		return nil, err
	}
	return []interface{}{id, name, hash, index}, nil
}

func scanTreeEntry(rows *sql.Rows) ([]interface{}, error) {
	var id, name, hash, tree, mode string
	if err := rows.Scan(&id, &name, &hash, &tree, &mode); err != nil {
		return nil, err
	}
	return []interface{}{id, name, hash, tree, mode}, nil
}

func scanTreeFile(rows *sql.Rows) ([]interface{}, error) {
	var id, tree, path, blob string
	var isVendor bool
	if err := rows.Scan(&id, &tree, &path, &blob, &isVendor); err != nil {
		return nil, err
	}
	return []interface{}{id, tree, path, blob, isVendor}, nil
}

func scanCommit(rows *sql.Rows) ([]interface{}, error) {
	var id, hash, authorName, authorMail string
	var committerName, committerMail, message, tree string
	var authorWhen, committerWhen time.Time
	var parents []string
	if err := rows.Scan(
		&id,
		&hash,
		&authorName,
		&authorMail,
		&authorWhen,
		&committerName,
		&committerMail,
		&committerWhen,
		&message,
		&tree,
		pq.Array(&parents),
	); err != nil {
		return nil, err
	}

	return []interface{}{
		id,
		hash,
		authorName,
		authorMail,
		authorWhen.Format(time.RFC3339),
		committerName,
		committerMail,
		committerWhen.Format(time.RFC3339),
		message,
		tree,
		parents,
	}, nil
}

func scanTreeBlob(rows *sql.Rows) ([]interface{}, error) {
	var id, tree, blob string
	if err := rows.Scan(&id, &tree, &blob); err != nil {
		return nil, err
	}
	return []interface{}{id, tree, blob}, nil
}

func scanBlob(rows *sql.Rows) ([]interface{}, error) {
	var id, hash string
	var size int64
	var content []byte
	var isBinary bool
	if err := rows.Scan(&id, &hash, &size, &content, &isBinary); err != nil {
		return nil, err
	}
	return []interface{}{id, hash, size, content, isBinary}, nil
}

func setupDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	dburl := os.Getenv("TESTDBURI")
	if dburl == "" {
		t.Skip("TESTDBURI not provided, skipping")
	}

	db, err := sql.Open("postgres", dburl)
	require.NoError(t, err)

	require.NoError(t, DropTables(db))
	require.NoError(t, CreateTables(db))

	return db, func() {
		require.NoError(t, DropTables(db))
	}
}

func setupRepo(t *testing.T) (borges.Repository, func()) {
	t.Helper()

	require.NoError(t, fixtures.Init())
	cleanup := func() {
		require.NoError(t, fixtures.Clean())
	}

	wt := fixtures.ByTag("worktree").One().Worktree()
	path := wt.Root()
	repo, err := git.PlainOpen(path)
	require.NoError(t, err)

	return testRepository{
		id:   "testrepo",
		repo: repo,
		fs:   wt,
	}, cleanup
}

type testRepository struct {
	id   string
	repo *git.Repository
	fs   billy.Filesystem
}

func (r testRepository) ID() borges.RepositoryID   { return borges.RepositoryID(r.id) }
func (r testRepository) Location() borges.Location { return nil }
func (r testRepository) Mode() borges.Mode         { return borges.ReadOnlyMode }
func (r testRepository) Commit() error {
	return nil
}
func (r testRepository) Close() error {
	return nil
}
func (r testRepository) R() *git.Repository {
	return r.repo
}
func (r testRepository) FS() billy.Filesystem {
	return r.fs
}

func setupMigrator(t *testing.T, db *sql.DB) *migrator {
	t.Helper()
	ctx := context.Background()
	var copiers = make(map[string]*tableCopier)
	for _, table := range tables {
		var err error
		copiers[table], err = newTableCopier(ctx, db, table)
		require.NoError(t, err)
	}

	return &migrator{
		db:           db,
		repositories: copiers["repositories"],
		refs:         copiers["refs"],
		refCommits:   copiers["ref_commits"],
		commits:      copiers["commits"],
		treeEntries:  copiers["tree_entries"],
		treeBlobs:    copiers["tree_blobs"],
		files:        copiers["tree_files"],
		blobs:        copiers["blobs"],
		repoWorkers:  runtime.NumCPU(),
	}
}
