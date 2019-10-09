# git2pg

Migrate git repositories to a PostgreSQL database.

## Status

git2pg is still under development and lacks both features and proper testing. Right now, its development has been focusing on making it perform well rather than correctness, which will come afterwards.

## Install

Manually using go get:

```
go get github.com/erizocosmico/git2pg/cmd/git2pg/...
```

Or manually building the binary by hand:

```
# at the repository root folder
go build -o git2pg ./cmd/git2pg/main.go
```

When the project is more stable, a pre-built binary will be provided in the releases page.

## Usage

To configure how git2pg works, you will need to use environment variables to specify the database details and command line flags to control certain aspects of the program.

### Environment variables

- `DBHOST`: PostgreSQL database host, `127.0.0.1` by default.
- `DBPORT`: PostgreSQL database port, `5432` by default.
- `DBUSER`: PostgreSQL database user, `postgres` by default.
- `DBPASS`: PostgreSQL database password, `` by default.
- `DBNAME`: PostgreSQL database name, `postgres` by default.

### Command line flags

- `-d <path>` path to the collection of repositories that will be migrated. For example, `-d /home/myuser/repos`. This must be a folder containing non-bare git repositories.
- `-siva` whether the collection of repositories are using the [siva archiving format](https://github.com/src-d/siva). Not enabled by default.
- `-rooted` whether the collection of repositories are rooted because they were collected with [gitcollector](https://github.com/src-d/gitcollector). Not enabled by default.
- `-buckets=N` number of characters for bucketing in case the repositories are in buckets. By default, `0`. For example, `-buckets=2` for a structure like the following:
```
|- go
   |- goofy
   |- goober
|- py
   |- pytorch
   |- pylint
```
- `-workers=N` number of parallel workers to use. This means, the number of repositories that will be migrated in parallel at the same time. By default is `cpu cores / 2`. Check out the note on worker numbers at the end of this section.
- `-repo-workers=N` number of workers to use while processing each single repository. By default is `cpu cores / 2`. Check out the note on worker numbers at the end of this section.
- `-v` verbose mode that will spit more logs. Only meant for debugging purposes. Not enabled by default.
- `-create` create the tables necessary in the schema.
- `-drop` drop the tables if they exist before creating them again. This option cannot be used unless `-create` is used as well.

**Note on setting worker numbers**

Since each repository can have more than one worker, you need to take into account that `WORKERS * REPOWORKERS` should be equal or lower to the number of cores of your machine.

For example, in a 32 core machine, where you want 2 repo workers per repository, you could have 16 workers, since 2 repo workers for each of the 16 workers is equal to the number of cores of the machine.

**Example of usage:**

```
git2pg -d /path/to/repos -workers=4 -repo-workers=2
```

## Schema

The schema is provided in `schema.sql` for reference purposes, but you can create it directly using the tool with the `-create` command line flag.

The schema contains the following tables:

- `repositories`: containing only ids of repositories.
- `refs`: containing the references of each repository and the commits they point to. References to objects other than commits are not included.
- `ref_commits`: which has each commit in each reference in each repository with a `history_index`, which is the offset to the HEAD of the reference.
- `commits`: containing all the commit information. Each table has the reference of the root tree at this point. That can be used to join with other tables that have information of root trees.
- `tree_entries`: containing all the tree entries in each repository. This table is not very useful, but migrated just to have that data that is in git.
- `tree_blobs`: containing the blob hashes that are in each root tree of each repository.
- `tree_blobs`: containing the files that are in each root tree of each repository.
- `blobs`: containing all the blobs in each repository, including its file content.

## LICENSE

Apache 2.0, see [LICENSE](/LICENSE)
