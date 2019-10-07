package git2pg

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

func visitRefCommits(
	ctx context.Context,
	repo borges.Repository,
	ref *plumbing.Reference,
	start *object.Commit,
	onCommitSeen func(*object.Commit) error,
	onRefCommitSeen func(*plumbing.Reference, plumbing.Hash, int) error,
	graphCache map[plumbing.Hash][]plumbing.Hash,
) error {
	return refCommitsVisitor{
		ctx,
		repo,
		ref,
		onCommitSeen,
		onRefCommitSeen,
		graphCache,
		make(map[plumbing.Hash]struct{}),
	}.visitCommits(start)
}

type refCommitsVisitor struct {
	ctx             context.Context
	repo            borges.Repository
	ref             *plumbing.Reference
	onCommitSeen    func(*object.Commit) error
	onRefCommitSeen func(*plumbing.Reference, plumbing.Hash, int) error
	graphCache      map[plumbing.Hash][]plumbing.Hash
	seen            map[plumbing.Hash]struct{}
}

type stackFrame struct {
	idx    int // idx from the start commit
	pos    int // pos in the hashes slice
	hashes []plumbing.Hash
}

func (r refCommitsVisitor) visitCommits(start *object.Commit) error {
	stack := []*stackFrame{
		{0, 0, []plumbing.Hash{start.Hash}},
	}
	for {
		if len(stack) == 0 {
			return nil
		}

		select {
		case <-r.ctx.Done():
			return context.Canceled
		default:
		}

		frame := stack[len(stack)-1]
		h := frame.hashes[frame.pos]
		frame.pos++
		if frame.pos >= len(frame.hashes) {
			stack = stack[:len(stack)-1]
		}

		if node, ok := r.graphCache[h]; ok {
			if err := r.visitCachedNode(frame.idx, h, node); err != nil {
				return err
			}
			continue
		}

		c, err := r.repo.R().CommitObject(h)
		if err != nil {
			return err
		}

		if err := r.onCommitSeen(c); err != nil {
			return err
		}

		r.seen[c.Hash] = struct{}{}
		r.graphCache[c.Hash] = c.ParentHashes

		if c.NumParents() > 0 {
			parents := make([]plumbing.Hash, 0, c.NumParents())
			for _, h = range c.ParentHashes {
				if _, ok := r.seen[h]; !ok {
					parents = append(parents, h)
				}
			}

			if len(parents) > 0 {
				stack = append(stack, &stackFrame{frame.idx + 1, 0, parents})
			}
		}

		if err := r.onRefCommitSeen(r.ref, c.Hash, frame.idx); err != nil {
			return err
		}
	}
}

func (r refCommitsVisitor) visitCachedNode(
	idx int,
	h plumbing.Hash,
	parents []plumbing.Hash,
) error {
	stack := []*stackFrame{
		{0, 0, []plumbing.Hash{h}},
	}

	for {
		if len(stack) == 0 {
			return nil
		}

		select {
		case <-r.ctx.Done():
			return context.Canceled
		default:
		}

		frame := stack[len(stack)-1]
		h := frame.hashes[frame.pos]
		frame.pos++
		if frame.pos >= len(frame.hashes) {
			stack = stack[:len(stack)-1]
		}

		r.seen[h] = struct{}{}

		var parents []plumbing.Hash
		for _, h = range r.graphCache[h] {
			if _, ok := r.seen[h]; !ok {
				parents = append(parents, h)
			}
		}

		if len(parents) > 0 {
			stack = append(stack, &stackFrame{frame.idx + 1, 0, parents})
		}

		if err := r.onRefCommitSeen(r.ref, h, frame.idx); err != nil {
			return err
		}
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
