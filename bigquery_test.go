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
	// ignore EOF
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
