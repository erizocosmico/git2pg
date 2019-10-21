package git2pg

import (
	"database/sql"
	"fmt"
)

// DropTables drops all the git2pg tables from the given database if they
// exist.
func DropTables(db *sql.DB) error {
	for table := range schema {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		if err != nil {
			return fmt.Errorf("could not drop table %s: %s", table, err)
		}
	}
	return nil
}

// CreateTables creates all git2pg tables in the given database if they
// do not exist already.
func CreateTables(db *sql.DB) error {
	for table, query := range schema {
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("could not create table %s: %s", table, err)
		}
	}
	return nil
}

var schema = map[string]string{
	"repositories": `CREATE TABLE IF NOT EXISTS repositories (
		repository_id TEXT NOT NULL
	)`,

	"refs": `CREATE TABLE IF NOT EXISTS refs (
		repository_id TEXT NOT NULL,
		ref_name TEXT NOT NULL,
		commit_hash VARCHAR(40) NOT NULL
	)`,

	"ref_commits": `CREATE TABLE IF NOT EXISTS ref_commits (
		repository_id TEXT NOT NULL,
		commit_hash VARCHAR(40) NOT NULL,
		ref_name TEXT NOT NULL,
		history_index BIGINT NOT NULL
	)`,

	"commits": `CREATE TABLE IF NOT EXISTS commits (
		repository_id TEXT NOT NULL,
		commit_hash VARCHAR(40) NOT NULL,
		commit_author_name TEXT NOT NULL,
		commit_author_email TEXT NOT NULL,
		commit_author_when timestamptz NOT NULL,
		committer_name TEXT NOT NULL,
		committer_email TEXT NOT NULL,
		committer_when timestamptz NOT NULL,
		commit_message TEXT NOT NULL,
		root_tree_hash VARCHAR(40) NOT NULL,
		commit_parents VARCHAR(40)[] NOT NULL
	)`,

	"tree_entries": `CREATE TABLE IF NOT EXISTS tree_entries (
		repository_id TEXT NOT NULL,
		tree_entry_name TEXT NOT NULL,
		blob_hash VARCHAR(40) NOT NULL,
		tree_hash VARCHAR(40) NOT NULL,
		tree_entry_mode VARCHAR(40) NOT NULL
	)`,

	"tree_files": `CREATE TABLE IF NOT EXISTS tree_files (
		repository_id TEXT NOT NULL,
		root_tree_hash VARCHAR(40) NOT NULL,
		file_path TEXT NOT NULL,
		blob_hash VARCHAR(40) NOT NULL,
		is_vendor BOOLEAN NOT NULL DEFAULT false
	)`,

	"tree_blobs": `CREATE TABLE IF NOT EXISTS tree_blobs (
		repository_id TEXT NOT NULL,
		root_tree_hash VARCHAR(40) NOT NULL,
		blob_hash VARCHAR(40) NOT NULL
	)`,

	"blobs": `CREATE TABLE IF NOT EXISTS blobs (
		repository_id TEXT NOT NULL,
		blob_hash VARCHAR(40) NOT NULL,
		blob_size BIGINT NOT NULL,
		blob_content BYTEA NOT NULL,
		is_binary BOOLEAN NOT NULL DEFAULT false
	)`,

	"remotes": `CREATE TABLE IF NOT EXISTS remotes (
		repository_id TEXT NOT NULL,
		remote_name TEXT NOT NULL,
		urls TEXT[] NOT NULL,
		fetch_refspecs TEXT[] NOT NULL
	)`,
}
