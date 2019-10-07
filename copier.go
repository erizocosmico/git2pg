package git2pg

import (
	"context"
	"database/sql"
	"fmt"
	"hash/crc64"
	"sync"
)

type tableCopier struct {
	ctx context.Context
	tx  *sql.Tx

	mut  sync.Mutex
	stmt *sql.Stmt

	cacheMut sync.RWMutex
	cache    map[uint64]struct{}
}

func newTableCopier(
	ctx context.Context,
	db *sql.DB,
	table string,
) (*tableCopier, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("cannot begin transaction: %s", err)
	}

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("COPY %s FROM STDIN", table))
	if err != nil {
		return nil, fmt.Errorf("cannot prepare statement: %s", err)
	}

	return &tableCopier{
		ctx:   ctx,
		tx:    tx,
		stmt:  stmt,
		cache: make(map[uint64]struct{}),
	}, nil
}

func (c *tableCopier) isCopied(hash uint64) bool {
	c.cacheMut.RLock()
	_, ok := c.cache[hash]
	c.cacheMut.RUnlock()
	return ok
}

// TODO(erizocosmico): remove hash and leave the uniqueness to the migrators
// themselves.
func (c *tableCopier) copy(hash uint64, values ...interface{}) error {
	if c.isCopied(hash) {
		return nil
	}

	c.mut.Lock()
	defer c.mut.Unlock()
	if _, err := c.stmt.ExecContext(c.ctx, values...); err != nil {
		return fmt.Errorf("cannot exec in copy operation: %s", err)
	}

	c.cacheMut.Lock()
	c.cache[hash] = struct{}{}
	c.cacheMut.Unlock()
	return nil
}

func (c *tableCopier) Close() error {
	if c.stmt != nil {
		if _, err := c.stmt.Exec(); err != nil {
			return fmt.Errorf("cannot exec in copy operation: %s", err)
		}
		c.stmt = nil
	}

	if c.tx != nil {
		if err := c.tx.Commit(); err != nil {
			return fmt.Errorf("cannot commit copy operation: %s", err)
		}
		c.tx = nil
	}

	return nil
}

var table = crc64.MakeTable(crc64.ISO)

func hash(elems ...interface{}) uint64 {
	return crc64.Checksum([]byte(fmt.Sprintf("%#v", elems)), table)
}
