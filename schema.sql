CREATE TABLE IF NOT EXISTS repositories (
  repository_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refs (
  repository_id TEXT NOT NULL,
  ref_name TEXT NOT NULL,
  commit_hash VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_commits (
  repository_id TEXT NOT NULL,
  commit_hash VARCHAR(40) NOT NULL,
  ref_name TEXT NOT NULL,
  history_index BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS commits (
  repository_id TEXT NOT NULL,
  commit_hash VARCHAR(40) NOT NULL,
  commit_author_name TEXT NOT NULL,
  commit_author_email TEXT NOT NULL,
  commit_author_when timestamp NOT NULL,
  committer_name TEXT NOT NULL,
  committer_email TEXT NOT NULL,
  committer_when timestamp NOT NULL,
  commit_message TEXT NOT NULL,
  root_tree_hash VARCHAR(40) NOT NULL,
  commit_parents VARCHAR(40)[] NOT NULL
);

CREATE TABLE IF NOT EXISTS tree_entries (
  repository_id TEXT NOT NULL,
  tree_entry_name TEXT NOT NULL,
  blob_hash VARCHAR(40) NOT NULL,
  tree_hash VARCHAR(40) NOT NULL,
  tree_entry_mode VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS tree_files (
  repository_id TEXT NOT NULL,
  root_tree_hash VARCHAR(40) NOT NULL,
  file_path TEXT NOT NULL,
  blob_hash VARCHAR(40) NOT NULL,
  is_vendor BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS tree_blobs (
  repository_id TEXT NOT NULL,
  root_tree_hash VARCHAR(40) NOT NULL,
  blob_hash VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs (
  repository_id TEXT NOT NULL,
  blob_hash VARCHAR(40) NOT NULL,
  blob_size BIGINT NOT NULL,
  blob_content BYTEA NOT NULL,
  is_binary BOOLEAN NOT NULL DEFAULT false
);
