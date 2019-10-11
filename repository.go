package git2pg

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

func newSyncRepository(repo borges.Repository) *syncRepository {
	return &syncRepository{Repository: repo}
}

type syncRepository struct {
	mut sync.Mutex
	borges.Repository
}

func (r *syncRepository) tree(hash plumbing.Hash) (*object.Tree, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.R().TreeObject(hash)
}

func (r *syncRepository) commit(hash plumbing.Hash) (*object.Commit, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.R().CommitObject(hash)
}

func (r *syncRepository) object(hash plumbing.Hash) (object.Object, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.R().Object(plumbing.AnyObject, hash)
}

func (r *syncRepository) blob(hash plumbing.Hash) (*object.Blob, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.R().BlobObject(hash)
}

func (r *syncRepository) blobContent(blob *object.Blob) ([]byte, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	rd, err := blob.Reader()
	if err != nil {
		return nil, fmt.Errorf("cannot get blob reader: %s", err)
	}

	content, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, fmt.Errorf("cannot read blob content: %s", err)
	}

	return content, nil
}

func (r *syncRepository) head() (*plumbing.Reference, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.R().Head()
}

func (r *syncRepository) refs() (*syncRefsIter, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	iter, err := r.R().References()
	if err != nil {
		return nil, err
	}
	return &syncRefsIter{&r.mut, iter}, nil
}

type syncRefsIter struct {
	mut  *sync.Mutex
	iter storer.ReferenceIter
}

func (i *syncRefsIter) Next() (*plumbing.Reference, error) {
	i.mut.Lock()
	defer i.mut.Unlock()
	return i.iter.Next()
}

func (i *syncRefsIter) Close() {
	i.mut.Lock()
	i.iter.Close()
	i.mut.Unlock()
}
