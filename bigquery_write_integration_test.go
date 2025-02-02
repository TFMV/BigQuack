package bigquack_test

import (
	"context"
	"testing"
	"time"

	BigQuack "github.com/TFMV/BigQuack"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestBigQueryWriteIntegration(t *testing.T) {
	//BIGQUERY_SERVICE_ACCOUNT_JSON := os.Getenv("BIGQUERY_SERVICE_ACCOUNT_JSON")
	//BIGQUERY_PROJECT := os.Getenv("BIGQUERY_PROJECT")
	//BIGQUERY_DATASET := os.Getenv("BIGQUERY_DATASET")
	//BIGQUERY_TABLE := os.Getenv("BIGQUERY_TABLE")

	BIGQUERY_SERVICE_ACCOUNT_JSON := "sa.json"
	BIGQUERY_PROJECT := "tfmv-371720"
	BIGQUERY_DATASET := "tfmv"
	BIGQUERY_TABLE := "users"

	if BIGQUERY_SERVICE_ACCOUNT_JSON == "" || BIGQUERY_PROJECT == "" || BIGQUERY_DATASET == "" || BIGQUERY_TABLE == "" {
		t.Skip("Skipping integration test; BIGQUERY_SERVICE_ACCOUNT_JSON, BIGQUERY_PROJECT, BIGQUERY_DATASET, and BIGQUERY_TABLE must be set")
	}

	ctx := context.Background()

	// Define a simple Arrow schema for writing.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	// Build a simple Arrow record with one row.
	alloc := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	intBuilder := builder.Field(0).(*array.Int32Builder)
	strBuilder := builder.Field(1).(*array.StringBuilder)
	intBuilder.AppendValues([]int32{1}, nil)
	strBuilder.AppendValues([]string{"Alice"}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Create the BigQuery record writer.
	writer, err := BigQuack.NewBigQueryManagedRecordWriter(ctx, BIGQUERY_SERVICE_ACCOUNT_JSON, schema, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE, nil)
	if err != nil {
		t.Fatalf("failed to create BigQueryRecordWriter: %v", err)
	}

	// Write the Arrow record to BigQuery.
	if err := writer.WriteRecord(record); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	// Finalize the write stream.
	if cnt, err := writer.Finalize(); err != nil {
		t.Fatalf("failed to finalize write stream: %v", err)
	} else {
		t.Logf("Wrote %d rows to BigQuery table %s.%s.%s", cnt, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
	}

	// Finalize and close the writer.
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	t.Logf("Record written successfully to BigQuery table %s.%s.%s at %s", BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE, time.Now().Format(time.RFC3339))
}
