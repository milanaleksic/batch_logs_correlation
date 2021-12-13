package main

import (
	"batch_to_sqlite"
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

type inputFiles []string

func (i *inputFiles) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *inputFiles) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var (
	inputFileBatchLocations inputFiles
	inputFileLogsLocations  inputFiles
	databaseLocation        string
)

func init() {
	var debug bool
	flag.Var(&inputFileBatchLocations, "input-file-batch", "input JSON files with AWS Batch summaries")
	flag.Var(&inputFileLogsLocations, "input-file-logs", "input CSV files with DD logs summaries")
	flag.StringVar(&databaseLocation, "database", "status.db", "SQLite3 database location")
	flag.BoolVar(&debug, "debug", false, "show debug messages")
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	if inputFileBatchLocations != nil {
		for _, inputFileLocation := range inputFileBatchLocations {
			if _, err := os.Stat(inputFileLocation); os.IsNotExist(err) {
				log.Fatalf("Input file does not exist: %s", inputFileLocation)
			}
		}
	}
}

func main() {
	db := prepareDatabase()
	defer func() {
		if err := db.Close(); err != nil {
			log.Errorf("Failed to close the database database: %v", err)
		}
	}()

	var openedFiles []io.Closer
	defer func() {
		for _, f := range openedFiles {
			err := f.Close()
			if err != nil {
				log.Warnf("Failed to close file %v, err=%v", f, err)
			}
		}
	}()

	ctx := context.Background()
	contentExtractor := batch_to_sqlite.NewExtractor(db)

	for _, inputFileLocation := range inputFileBatchLocations {
		f, err := os.Open(inputFileLocation)
		if err != nil {
			log.Fatalf("Failed to open input file: %s, reason: %v", inputFileLocation, err)
		}
		openedFiles = append(openedFiles, f)

		if err = contentExtractor.IngestBatchRecords(ctx, f); err != nil {
			log.Fatalf("failed to ingest records for %v: %v", inputFileLocation, err)
		}
	}

	for _, inputFileLocation := range inputFileLogsLocations {
		f, err := os.Open(inputFileLocation)
		if err != nil {
			log.Fatalf("Failed to open input file: %s, reason: %v", inputFileLocation, err)
		}
		openedFiles = append(openedFiles, f)

		if err = contentExtractor.IngestLogRecords(ctx, f); err != nil {
			log.Fatalf("failed to ingest records for %v: %v", inputFileLocation, err)
		}
	}
}

func prepareDatabase() *sql.DB {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	return db
}
