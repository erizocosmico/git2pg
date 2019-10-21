package git2pg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lib/pq"
	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type refCommitsListener struct {
	repo             borges.Repository
	refHash          plumbing.Hash
	full             bool
	refCommits       *tableCopier
	commits          *tableCopier
	onCommitTreeSeen func(plumbing.Hash)
}

func (l *refCommitsListener) onRefCommitSeen(
	ref *plumbing.Reference,
	commit plumbing.Hash,
	idx int,
) error {
	return l.refCommits.copy(
		l.repo.ID(),
		commit.String(),
		ref.Name().String(),
		uint64(idx),
	)
}

func (l *refCommitsListener) onCommitSeen(c *object.Commit) error {
	if l.full || l.refHash == c.Hash {
		l.onCommitTreeSeen(c.TreeHash)
	}

	parents := make([]string, len(c.ParentHashes))
	for i, h := range c.ParentHashes {
		parents[i] = h.String()
	}

	values := []interface{}{
		l.repo.ID(),
		c.Hash.String(),
		sanitizeString(c.Author.Name),
		sanitizeString(c.Author.Email),
		c.Author.When.Format(time.RFC3339),
		sanitizeString(c.Committer.Name),
		sanitizeString(c.Committer.Email),
		c.Committer.When.Format(time.RFC3339),
		sanitizeString(c.Message),
		c.TreeHash.String(),
		pq.Array(parents),
	}

	if err := l.commits.copy(values...); err != nil {
		return fmt.Errorf("cannot copy commit %s: %s", c.Hash, err)
	}

	return nil
}

func visitRefCommits(
	ctx context.Context,
	repo *syncRepository,
	ref *plumbing.Reference,
	start *object.Commit,
	listener *refCommitsListener,
	graphCache *graphCache,
) error {
	return (&refCommitsVisitor{
		ctx:        ctx,
		repo:       repo,
		ref:        ref,
		listener:   listener,
		graphCache: graphCache,
		seen:       make(map[plumbing.Hash]struct{}),
	}).visitCommits(0, start.Hash)
}

type refCommitsVisitor struct {
	ctx        context.Context
	repo       *syncRepository
	ref        *plumbing.Reference
	listener   *refCommitsListener
	graphCache *graphCache
	seen       map[plumbing.Hash]struct{}
	mut        sync.RWMutex
}

type stackFrame struct {
	idx    int // idx from the start commit
	pos    int // pos in the hashes slice
	hashes []plumbing.Hash
}

func (r *refCommitsVisitor) visitCommits(idx int, start plumbing.Hash) error {
	stack := []*stackFrame{
		{idx, 0, []plumbing.Hash{start}},
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
		r.seen[h] = struct{}{}
		frame.pos++
		if frame.pos >= len(frame.hashes) {
			stack = stack[:len(stack)-1]
		}

		if _, ok := r.graphCache.parents(h); ok {
			if err := r.visitCachedNode(frame.idx, h); err != nil {
				return err
			}
			continue
		}

		c, err := r.repo.commit(h)
		if err != nil {
			return err
		}

		if _, ok := r.graphCache.parents(c.Hash); !ok {
			if err := r.listener.onCommitSeen(c); err != nil {
				return err
			}
		}

		r.graphCache.put(c.Hash, c.ParentHashes)

		if c.NumParents() > 0 {
			parents := make([]plumbing.Hash, 0, c.NumParents())
			for _, h := range c.ParentHashes {
				if _, ok := r.seen[h]; !ok {
					parents = append(parents, h)
					r.seen[h] = struct{}{}
				}
			}

			if len(parents) > 0 {
				stack = append(stack, &stackFrame{frame.idx + 1, 0, parents})
			}
		}

		err = r.listener.onRefCommitSeen(r.ref, c.Hash, frame.idx)
		if err != nil {
			return err
		}
	}
}

func (r *refCommitsVisitor) visitCachedNode(
	idx int,
	h plumbing.Hash,
) error {
	stack := []*stackFrame{
		{idx, 0, []plumbing.Hash{h}},
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
		r.seen[h] = struct{}{}
		frame.pos++
		if frame.pos >= len(frame.hashes) {
			stack = stack[:len(stack)-1]
		}

		if _, ok := r.graphCache.parents(h); !ok {
			if err := r.visitCommits(frame.idx, h); err != nil {
				return err
			}
			continue
		}

		parents, _ := r.graphCache.parents(h)
		if len(parents) > 0 {
			ps := make([]plumbing.Hash, 0, len(parents))
			for _, h := range parents {
				if _, ok := r.seen[h]; !ok {
					ps = append(ps, h)
					r.seen[h] = struct{}{}
				}
			}

			if len(ps) > 0 {
				stack = append(stack, &stackFrame{frame.idx + 1, 0, ps})
			}
		}

		if err := r.listener.onRefCommitSeen(r.ref, h, frame.idx); err != nil {
			return err
		}
	}
}

func isIgnoredReference(r *plumbing.Reference) bool {
	return r.Type() != plumbing.HashReference
}

var errInvalidCommit = errors.New("invalid commit")

func resolveCommit(repo *syncRepository, hash plumbing.Hash) (*object.Commit, error) {
	obj, err := repo.object(hash)
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

type graphCache struct {
	mut   sync.RWMutex
	cache map[plumbing.Hash][]plumbing.Hash
}

func newGraphCache() *graphCache {
	return &graphCache{cache: make(map[plumbing.Hash][]plumbing.Hash)}
}

func (g *graphCache) parents(hash plumbing.Hash) ([]plumbing.Hash, bool) {
	g.mut.RLock()
	parents, ok := g.cache[hash]
	g.mut.RUnlock()
	return parents, ok
}

func (g *graphCache) put(hash plumbing.Hash, parents []plumbing.Hash) {
	g.mut.Lock()
	g.cache[hash] = parents
	g.mut.Unlock()
}
