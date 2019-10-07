package git2pg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type commitNotifier func(*object.Commit) error

func copyCommitNotifier(copier *tableCopier, r borges.Repository) commitNotifier {
	return func(c *object.Commit) error {
		hash := hash(r.ID(), c.Hash.String())
		if copier.isCopied(hash) {
			return nil
		}

		parentHashes := make([]string, len(c.ParentHashes))
		for i, h := range c.ParentHashes {
			parentHashes[i] = h.String()
		}

		parents, err := json.Marshal(parentHashes)
		if err != nil {
			return fmt.Errorf("cannot marshal commit parents: %s", err)
		}

		values := []interface{}{
			r.ID(),
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

		if err := copier.copy(hash, values...); err != nil {
			return fmt.Errorf("cannot copy commit %s: %s", c.Hash, err)
		}

		return nil
	}
}

type indexedCommitIter struct {
	repo     borges.Repository
	stack    []*stackFrame
	seen     map[plumbing.Hash]struct{}
	notifier commitNotifier
}

func newIndexedCommitIter(
	repo borges.Repository,
	start *object.Commit,
	commitNotifier commitNotifier,
) *indexedCommitIter {
	return &indexedCommitIter{
		repo: repo,
		stack: []*stackFrame{
			{0, 0, []plumbing.Hash{start.Hash}},
		},
		seen:     make(map[plumbing.Hash]struct{}),
		notifier: commitNotifier,
	}
}

type stackFrame struct {
	idx    int // idx from the start commit
	pos    int // pos in the hashes slice
	hashes []plumbing.Hash
}

func (i *indexedCommitIter) next() (*object.Commit, int, error) {
	for {
		if len(i.stack) == 0 {
			return nil, -1, io.EOF
		}

		frame := i.stack[len(i.stack)-1]

		h := frame.hashes[frame.pos]
		if _, ok := i.seen[h]; !ok {
			i.seen[h] = struct{}{}
		}

		frame.pos++
		if frame.pos >= len(frame.hashes) {
			i.stack = i.stack[:len(i.stack)-1]
		}

		c, err := i.repo.R().CommitObject(h)
		if err != nil {
			return nil, -1, err
		}

		if err := i.notifier(c); err != nil {
			return nil, -1, err
		}

		if c.NumParents() > 0 {
			parents := make([]plumbing.Hash, 0, c.NumParents())
			for _, h = range c.ParentHashes {
				if _, ok := i.seen[h]; !ok {
					parents = append(parents, h)
				}
			}

			if len(parents) > 0 {
				i.stack = append(i.stack, &stackFrame{frame.idx + 1, 0, parents})
			}
		}

		return c, frame.idx, nil
	}
}

func isIgnoredReference(r *plumbing.Reference) bool {
	return r.Type() != plumbing.HashReference
}

var errInvalidCommit = errors.New("invalid commit")

func resolveCommit(repo *git.Repository, hash plumbing.Hash) (*object.Commit, error) {
	obj, err := repo.Object(plumbing.AnyObject, hash)
	if err != nil {
		return nil, err
	}

	switch obj := obj.(type) {
	case *object.Commit:
		return obj, nil
	case *object.Tag:
		return resolveCommit(repo, obj.Target)
	default:
		return nil, errInvalidCommit
	}
}

func sanitizeString(s string) string {
	if !utf8.ValidString(s) {
		v := make([]rune, 0, len(s))
		for i, r := range s {
			if r == utf8.RuneError {
				_, size := utf8.DecodeRuneInString(s[i:])
				if size == 1 {
					continue
				}
			}
			v = append(v, r)
		}
		s = string(v)
	}

	return s
}
