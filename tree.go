package git2pg

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

func visitFileTree(
	ctx context.Context,
	repo *syncRepository,
	origin plumbing.Hash,
	onTreeEntrySeen func(plumbing.Hash, object.TreeEntry) error,
	onTreeBlobSeen func(tree, blob plumbing.Hash) error,
	onFileSeen func(path string, tree plumbing.Hash, blob plumbing.Hash) error,
	onBlobSeen func(*object.Blob, []byte) error,
	treesCache *treesCache,
) error {
	return fileTreeVisitor{
		repo,
		origin,
		onTreeEntrySeen,
		onTreeBlobSeen,
		onFileSeen,
		onBlobSeen,
		treesCache,
	}.visitTree(ctx, origin, "")
}

type fileTreeVisitor struct {
	repo            *syncRepository
	origin          plumbing.Hash
	onTreeEntrySeen func(plumbing.Hash, object.TreeEntry) error
	onTreeBlobSeen  func(tree, blob plumbing.Hash) error
	onFileSeen      func(path string, tree plumbing.Hash, blob plumbing.Hash) error
	onBlobSeen      func(*object.Blob, []byte) error
	cache           *treesCache
}

func (f fileTreeVisitor) visitTree(
	ctx context.Context,
	hash plumbing.Hash,
	path string,
) error {
	select {
	case <-ctx.Done():
		return context.Canceled
	default:
	}

	if tree, ok := f.cache.tree(hash); ok {
		return f.visitCachedTree(ctx, path, tree)
	}

	t, err := f.repo.tree(hash)
	if err != nil {
		return fmt.Errorf("cannot get tree: %s", err)
	}

	node := &treeNode{
		name:  lastPathElement(path),
		isDir: true,
	}

	for _, entry := range t.Entries {
		if err := f.visitEntry(ctx, node, hash, entry, path); err != nil {
			return fmt.Errorf("cannot visit entry: %s", err)
		}
	}

	f.cache.putTree(hash, node)

	return nil
}

func (f fileTreeVisitor) visitCachedTree(
	ctx context.Context,
	path string,
	tree *treeNode,
) error {
	for _, entry := range tree.entries {
		if entry.isDir {
			if err := f.visitCachedTree(ctx, join(path, entry.name), entry); err != nil {
				return err
			}
		} else {
			if err := f.onFileSeen(join(path, entry.name), f.origin, entry.hash); err != nil {
				return err
			}

			if err := f.onTreeBlobSeen(f.origin, entry.hash); err != nil {
				return err
			}
		}
	}

	return nil
}

func (f fileTreeVisitor) visitEntry(
	ctx context.Context,
	node *treeNode,
	tree plumbing.Hash,
	entry object.TreeEntry,
	path string,
) error {
	select {
	case <-ctx.Done():
		return context.Canceled
	default:
	}

	if !f.cache.entrySeen(tree, entry.Name) {
		if err := f.onTreeEntrySeen(tree, entry); err != nil {
			return err
		}
	}

	path = join(path, "/", entry.Name)
	if entry.Mode == filemode.Dir {
		if err := f.visitTree(ctx, entry.Hash, path); err != nil {
			return err
		}

		tree, _ := f.cache.tree(entry.Hash)
		node.entries = append(node.entries, tree)
	} else {
		if err := f.visitFile(entry.Hash, path); err != nil {
			return err
		}
		node.entries = append(node.entries, &treeNode{
			name:  entry.Name,
			isDir: false,
			hash:  entry.Hash,
		})
	}

	return nil
}

func (f fileTreeVisitor) visitFile(hash plumbing.Hash, path string) error {
	if err := f.onFileSeen(path, f.origin, hash); err != nil {
		return err
	}

	if err := f.onTreeBlobSeen(f.origin, hash); err != nil {
		return err
	}

	if f.cache.blobSeen(hash) {
		return nil
	}

	blob, err := f.repo.blob(hash)
	if err != nil {
		return fmt.Errorf("could not get blob: %s", err)
	}

	content, err := f.repo.blobContent(blob)
	if err != nil {
		return err
	}

	return f.onBlobSeen(blob, content)
}

const sniffLen = 8000

// isBinary detects if data is a binary value based on:
// http://git.kernel.org/cgit/git/git.git/tree/xdiff-interface.c?id=HEAD#n198
func isBinary(content []byte) bool {
	rd := bufio.NewReader(bytes.NewReader(content))
	var i int
	for {
		if i >= sniffLen {
			return false
		}
		i++

		b, err := rd.ReadByte()
		if err == io.EOF {
			return false
		}

		if err != nil {
			return false
		}

		if b == 0 {
			return true
		}
	}
}

type treeNode struct {
	name    string
	isDir   bool
	hash    plumbing.Hash
	entries []*treeNode
}

func lastPathElement(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func join(parts ...string) string {
	var p []string
	for _, part := range parts {
		if part != "" {
			p = append(p, part)
		}
	}
	return strings.Join(p, "/")
}

type entry struct {
	tree plumbing.Hash
	name string
}

type treesCache struct {
	blobsMut  sync.RWMutex
	seenBlobs map[plumbing.Hash]struct{}

	entriesMut  sync.RWMutex
	seenEntries map[entry]struct{}

	treesMut sync.RWMutex
	trees    map[plumbing.Hash]*treeNode
}

func newTreesCache() *treesCache {
	return &treesCache{
		seenBlobs:   make(map[plumbing.Hash]struct{}),
		seenEntries: make(map[entry]struct{}),
		trees:       make(map[plumbing.Hash]*treeNode),
	}
}

func (c *treesCache) putTree(hash plumbing.Hash, node *treeNode) {
	c.treesMut.Lock()
	c.trees[hash] = node
	c.treesMut.Unlock()
}

func (c *treesCache) tree(hash plumbing.Hash) (*treeNode, bool) {
	c.treesMut.RLock()
	node, ok := c.trees[hash]
	c.treesMut.RUnlock()
	return node, ok
}

func (c *treesCache) blobSeen(hash plumbing.Hash) (seenBefore bool) {
	c.blobsMut.Lock()
	_, ok := c.seenBlobs[hash]
	if !ok {
		c.seenBlobs[hash] = struct{}{}
	}
	c.blobsMut.Unlock()
	return ok
}

func (c *treesCache) entrySeen(hash plumbing.Hash, name string) (seenBefore bool) {
	c.entriesMut.Lock()
	_, ok := c.seenEntries[entry{hash, name}]
	if !ok {
		c.seenEntries[entry{hash, name}] = struct{}{}
	}
	c.entriesMut.Unlock()
	return ok
}
