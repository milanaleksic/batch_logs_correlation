package batch_to_sqlite

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"regexp"
	"time"
)

var uuidMatchingMachine = regexp.MustCompile("[^-]{8}-[^-]{4}-[^-]{4}-[^-]{4}-[^-]{12}")

type JobSummary struct {
	JobId        string `json:"jobId"`
	JobName      string `json:"jobName"`
	CreatedAt    int64  `json:"createdAt"`
	Status       string `json:"status"`
	StatusReason string `json:"statusReason"`
	StartedAt    int64  `json:"startedAt"`
	StoppedAt    int64  `json:"stoppedAt"`
	Container    struct {
		ExitCode int `json:"exitCode"`
	} `json:"container"`
}

type StatusesFile struct {
	JobSummaryList []JobSummary `json:"jobSummaryList"`
}

type Extractor struct {
	db *sql.DB
}

func NewExtractor(db *sql.DB) *Extractor {
	sqlStmt := `
	drop table if exists batch;
	create table if not exists batch (
		externalId text not null, 
		id text not null,
		name text not null,
		created timestamp,
		started timestamp,
		stopped timestamp,
		status text,
		statusReason text
	);
	drop table if exists log;
	create table if not exists log (
		id text not null,
		ts timestamp,
	    service text, 
	    thread text
	);
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}
	return &Extractor{
		db: db,
	}
}

func (e Extractor) readBatchRecordIntoDB(s JobSummary) error {
	tx, err := e.db.Begin()
	MustCheck(err)
	stmt, err := tx.Prepare("insert into batch(externalId, id, name, created, started, stopped, status, statusReason) values(?,?,?,?,?,?,?,?)")
	MustCheck(err)
	defer SafeClose(stmt, &err)

	matchId := uuidMatchingMachine.FindString(s.JobName)
	if matchId == "" {
		log.Fatalf("Could not identify the internal ID from the batch job name")
	}

	_, err = stmt.Exec(
		s.JobId,
		matchId,
		s.JobName,
		time.UnixMilli(s.CreatedAt),
		time.UnixMilli(s.StartedAt),
		time.UnixMilli(s.StoppedAt),
		s.Status,
		s.StatusReason,
	)
	if err != nil {
		return fmt.Errorf("failed to retrieve last inserted book: %w", err)
	}
	MustCheck(tx.Commit())
	return nil
}

func (e Extractor) IngestBatchRecords(ctx context.Context, f *os.File) (err error) {
	s, err := e.parseInput(err, f)
	if err != nil {
		return
	}
	for _, s := range s.JobSummaryList {
		if errLoading := e.readBatchRecordIntoDB(s); errLoading != nil {
			log.Errorf("failed to read a record into the database: %v, %w", s, errLoading)
		}
	}
	return nil
}

func (e Extractor) parseInput(err error, f *os.File) (StatusesFile, error) {
	s := StatusesFile{}
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return StatusesFile{}, fmt.Errorf("unexpected problem: could not read from file %+v: %v", f.Name(), err)
	}
	err = json.Unmarshal(bytes, &s)
	if err != nil {
		return StatusesFile{}, fmt.Errorf("unexpected problem: could not deserialize from JSON %+v: %v", f.Name(), err)
	}
	log.Infof("Read %d batch records", len(s.JobSummaryList))
	return s, err
}

const fieldDate = "date"
const fieldService = "Service"
const fieldThread = "@thread_name"
const fieldMessage = "message"

// Expected input content:
//
//     date,Service,@thread_name,message
//     2021-12-11T06:25:15.107Z,shape-server-worker-cloud,AsyncRequestMessageListener-1,Submitting job [c86a5ae7-3d84-405e-be0d-5936bbb18ab3] to AWS Batch
func (e Extractor) IngestLogRecords(ctx context.Context, f *os.File) error {
	reader := csv.NewReader(f)
	all, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read as CSV the file %v: err=%v", f, err)
	}

	var header = make(map[string]int)
	for i, x := range all {
		if i == 0 {
			makeHeader(x, header)
			continue
		}
		ts, err := time.Parse("2006-01-02T15:04:05.999Z", x[header[fieldDate]])
		if err != nil {
			log.Fatalf("Failed to parse timestamp rowNumber=%v, row=%v, err=%v", i+1, x, err)
		}
		id := uuidMatchingMachine.FindString(x[header[fieldMessage]])

		service := x[header[fieldService]]
		thread := x[header[fieldThread]]
		if errLoading := e.readLogRecordIntoDB(id, ts, service, thread); errLoading != nil {
			log.Errorf("failed to read a record into the database: %w", errLoading)
		}
	}
	log.Infof("Read %d log records", len(all)-1)
	return nil
}

func (e Extractor) readLogRecordIntoDB(id string, ts time.Time, service string, thread string) error {
	tx, err := e.db.Begin()
	MustCheck(err)
	stmt, err := tx.Prepare("insert into log(id, ts, service, thread) values(?,?,?,?)")
	MustCheck(err)
	defer SafeClose(stmt, &err)
	_, err = stmt.Exec(
		id,
		ts,
		service,
		thread,
	)
	if err != nil {
		return fmt.Errorf("failed to retrieve last inserted book: %w", err)
	}
	MustCheck(tx.Commit())
	return nil
}

func makeHeader(x []string, header map[string]int) {
	for j, h := range x {
		header[h] = j
	}

	var ok bool
	_, ok = header[fieldDate]
	if !ok {
		log.Fatalf("Field %v not found in header %v", fieldDate, header)
	}
	_, ok = header[fieldService]
	if !ok {
		log.Fatalf("Field %v not found in header %v", fieldService, header)
	}
	_, ok = header[fieldThread]
	if !ok {
		log.Fatalf("Field %v not found in header %v", fieldThread, header)
	}
	_, ok = header[fieldMessage]
	if !ok {
		log.Fatalf("Field %v not found in header %v", fieldMessage, header)
	}
}
