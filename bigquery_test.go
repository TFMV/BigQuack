package bigquack_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	bigquack "github.com/TFMV/BigQuack"
)

type BigQueryReaderOptions struct {
	MaxStreamCount int32
}

func TestBigQuery(t *testing.T) {
	bq, err := bigquack.NewBigQueryReadClient(context.Background())
	if err != nil {
		t.Fatalf("failed to create BigQuery client: %v", err)
	}

	opts := &bigquack.BigQueryReaderOptions{
		MaxStreamCount: 1,
	}

	reader, err := bq.NewBigQueryReader(context.Background(), "tfmv-371720", "tpch", "nation", opts)
	if err != nil {
		t.Fatalf("failed to create BigQuery reader: %v", err)
	}
	defer reader.Close()
	for {
		_, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to read record: %v", err)
		}
	}
}

func TestIntegration(t *testing.T) {
	db, err := bigquack.NewDuckDB()
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}
	defer db.Close()

	conn, err := db.OpenConnection()
	if err != nil {
		t.Fatalf("failed to create DuckDB connection: %v", err)
	}
	defer conn.Close()

	// Fetch table data from BigQuery and ingest into DuckDB
	bq, err := bigquack.NewBigQueryReadClient(context.Background())
	if err != nil {
		t.Fatalf("failed to create BigQuery client: %v", err)
	}
	opts := &bigquack.BigQueryReaderOptions{
		MaxStreamCount: 2,
	}
	reader, err := bq.NewBigQueryReader(context.Background(), "tfmv-371720", "tpch", "supplier", opts)
	if err != nil {
		t.Fatalf("failed to create BigQuery reader: %v", err)
	}
	defer reader.Close()

	rec, err := reader.Read()
	if err != nil {
		t.Fatalf("failed to read record: %v", err)
	}
	conn.IngestCreateAppend(context.Background(), "supplier", rec)

	// Query DuckDB and print results
	rows, _, _, err := conn.Query(context.Background(), "SELECT * FROM supplier")
	if err != nil {
		t.Fatalf("failed to query DuckDB: %v", err)
	}
	defer rows.Release()

	var totalRows int64
	for rows.Next() {
		rec := rows.Record()
		totalRows += int64(rec.NumRows())
	}
	fmt.Println("Total rows:", totalRows)
}
