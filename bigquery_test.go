package bigquack_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	bigquack "github.com/TFMV/BigQuack"
)

func TestBigQuery(t *testing.T) {
	bq, err := bigquack.NewBigQueryReadClient(context.Background())
	if err != nil {
		t.Fatalf("failed to create BigQuery client: %v", err)
	}
	reader, err := bq.NewBigQueryReader(context.Background(), "tfmv-371720", "tpch", "nation")
	if err != nil {
		t.Fatalf("failed to create BigQuery reader: %v", err)
	}
	defer reader.Close()
	for {
		rec, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to read record: %v", err)
		}
		fmt.Println(rec)
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
	reader, err := bq.NewBigQueryReader(context.Background(), "tfmv-371720", "tpch", "nation")
	if err != nil {
		t.Fatalf("failed to create BigQuery reader: %v", err)
	}
	defer reader.Close()

	rec, err := reader.Read()
	if err != nil {
		t.Fatalf("failed to read record: %v", err)
	}
	conn.IngestCreateAppend(context.Background(), "nation", rec)

	// Query DuckDB and print results
	rows, _, _, err := conn.Query(context.Background(), "SELECT * FROM nation")
	if err != nil {
		t.Fatalf("failed to query DuckDB: %v", err)
	}

	for rows.Next() {
		rec := rows.Record()
		fmt.Println(rec)
	}
}
