package git2pg

import (
	"fmt"
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type refIterator struct {
	repo *syncRepository
	iter *syncRefsIter
	head *plumbing.Reference
}

func (i *refIterator) close() {
	if i.iter != nil {
		i.iter.Close()
	}
}

func (i *refIterator) next() (*plumbing.Reference, *object.Commit, error) {
	for {
		if i.iter == nil {
			var err error
			i.iter, err = i.repo.refs()
			if err != nil {
				return nil, nil, fmt.Errorf("cannot get references: %s", err)
			}

			i.head, err = i.repo.head()
			if err != nil && err != plumbing.ErrReferenceNotFound {
				return nil, nil, fmt.Errorf("cannot get HEAD reference: %s", err)
			}
		}

		var ref *plumbing.Reference
		if i.head != nil {
			ref = plumbing.NewHashReference("HEAD", i.head.Hash())
			i.head = nil
		} else {
			var err error
			ref, err = i.iter.Next()
			if err != nil {
				if err == io.EOF {
					return nil, nil, io.EOF
				}

				return nil, nil, fmt.Errorf("cannot get next reference: %s", err)
			}

			if isIgnoredReference(ref) {
				continue
			}
		}

		commit, err := resolveCommit(i.repo, ref.Hash())
		if err != nil {
			if err == errInvalidCommit {
				continue
			}

			return nil, nil, fmt.Errorf("cannot resolve commit: %s", err)
		}

		return ref, commit, nil
	}
}
