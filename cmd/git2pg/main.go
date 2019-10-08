package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"

	"github.com/erizocosmico/git2pg"
	"github.com/sirupsen/logrus"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4/osfs"

	_ "github.com/lib/pq"
)

func main() {
	var path string
	var useSiva, rooted, verbose bool
	var buckets, workers, repoWorkers int

	flag.StringVar(&path, "d", "", "path to the repositories library")
	flag.BoolVar(&useSiva, "siva", false, "use siva repositories")
	flag.BoolVar(&rooted, "rooted", false, "use rooted repositories")
	flag.IntVar(&buckets, "buckets", 0, "number of characters of buckets in the repository library")
	flag.IntVar(&workers, "workers", runtime.NumCPU()/2, "workers to use")
	flag.IntVar(&repoWorkers, "repo-workers", runtime.NumCPU()/2, "workers to use for processing each repository")
	flag.BoolVar(&verbose, "v", false, "verbose mode")
	flag.Parse()

	if verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if fi, err := os.Stat(path); err != nil {
		logrus.Fatalf("error with given library path %q: %s", path, err)
	} else if !fi.IsDir() {
		logrus.Fatalf("given library path %q is not a directory", path)
	}

	fs := osfs.New(path)
	var lib borges.Library
	if useSiva {
		var err error
		lib, err = siva.NewLibrary("library", fs, &siva.LibraryOptions{
			Bucket:        buckets,
			RootedRepo:    rooted,
			Transactional: true,
		})

		if err != nil {
			logrus.Fatalf("unable to initialize siva library: %s", err)
		}
	} else {
		plib := plain.NewLibrary("library", &plain.LibraryOptions{})

		loc, err := plain.NewLocation(
			borges.LocationID(path),
			fs,
			&plain.LocationOptions{
				Performance: true,
				Bare:        false,
			})
		if err != nil {
			logrus.Fatalf("cannot add location: %s", err)
		}

		plib.AddLocation(loc)
		lib = plib
	}

	db, err := sql.Open("postgres", connectionString())
	if err != nil {
		logrus.Fatalf("unable to open connection to database: %s", err)
	}

	if err := db.Ping(); err != nil {
		logrus.Fatalf("unable to connect to database: %s", err)
	}

	logrus.WithField("workers", workers).Info("migrating library to database")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		cancel()
	}()

	if err := git2pg.Migrate(ctx, lib, db, workers, repoWorkers); err != nil {
		logrus.Fatalf("unable to migrate library to database: %s", err)
	}

	logrus.Info("library migrated successfully to database")
}

func connectionString() string {
	var (
		dbUser = envOrDefault("DBUSER", "root")
		dbPass = envOrDefault("DBPASS", "")
		dbHost = envOrDefault("DBHOST", "127.0.0.1")
		dbPort = envOrDefault("DBPORT", "5432")
		dbName = envOrDefault("DBNAME", "")
	)

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		dbUser, dbPass, dbHost, dbPort, dbName,
	)
}

func envOrDefault(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultValue
}
