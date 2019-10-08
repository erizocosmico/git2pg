package git2pg

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type syncRepository struct {
	mut sync.Mutex
	borges.Repository
}

func (r *syncRepository) tree(hash plumbing.Hash) (*object.Tree, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.R().TreeObject(hash)
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
