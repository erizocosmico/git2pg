package git2pg

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

type tableCopier struct {
	ctx context.Context
	tx  *sql.Tx

	mut  sync.Mutex
	stmt *sql.Stmt
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
		ctx:  ctx,
		tx:   tx,
		stmt: stmt,
	}, nil
}

func (c *tableCopier) copy(values ...interface{}) error {

	c.mut.Lock()
	defer c.mut.Unlock()
	if _, err := c.stmt.ExecContext(c.ctx, values...); err != nil {
		return fmt.Errorf("cannot exec in copy operation: %s", err)
	}

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
