package bigquack_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	BigQuack "github.com/TFMV/BigQuack"
	"github.com/TFMV/arrowpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestBigQueryWriteIntegration(t *testing.T) {
	// Use environment variables or constants.
	BIGQUERY_SERVICE_ACCOUNT_JSON := "sa.json"
	BIGQUERY_PROJECT := "tfmv-371720"
	BIGQUERY_DATASET := "tfmv"
	BIGQUERY_TABLE := "foo"

	// Read the service account JSON from file.
	creds, err := os.ReadFile(BIGQUERY_SERVICE_ACCOUNT_JSON)
	if err != nil {
		t.Fatalf("failed to read service account JSON file: %v", err)
	}

	if string(creds) == "" || BIGQUERY_PROJECT == "" || BIGQUERY_DATASET == "" || BIGQUERY_TABLE == "" {
		t.Skip("Skipping integration test; BIGQUERY_SERVICE_ACCOUNT_JSON, BIGQUERY_PROJECT, BIGQUERY_DATASET, and BIGQUERY_TABLE must be set")
	}

	ctx := context.Background()

	// Define a simple Arrow schema for writing.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.BinaryTypes.String},
	}, nil)

	// Build a simple Arrow record with one row.
	alloc := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	strBuilder := builder.Field(0).(*array.StringBuilder)
	strBuilder.AppendValues([]string{"Alice"}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Use ConvertConfig with wrapper types disabled
	cfg := arrowpb.ConvertConfig{
		UseWrapperTypes:        false,
		UseWellKnownTimestamps: false,
		UseProto2Syntax:        true,
	}

	// (Optional) Log the generated descriptor for debugging.
	fdp, err := arrowpb.ArrowSchemaToFileDescriptorProto(schema, "bigquack", "ManagedMsg", &cfg)
	if err != nil {
		t.Fatalf("failed to convert Arrow schema to FileDescriptorProto: %v", err)
	}
	t.Logf("Generated FileDescriptorProto: %v", fdp)

	// Create the BigQuery record writer.
	writer, err := BigQuack.NewBigQueryManagedRecordWriter(ctx, string(creds), schema, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE, &BigQuack.BigQueryWriteOptions{
		WriteStreamType: storagepb.WriteStream_COMMITTED,
		DataFormat:      storagepb.DataFormat_ARROW,
	}, option.WithCredentialsJSON(creds))
	if err != nil {
		t.Fatalf("failed to create BigQueryRecordWriter: %v", err)
	}

	err = writer.WriteRecord(record)
	if err != nil {
		fmt.Printf("Append error: %v\n", err)
		st, ok := status.FromError(err)
		if ok {
			fmt.Printf("Error status: %v\n", st.Code())
			for _, detail := range st.Details() {
				fmt.Printf("Row error detail: %v\n", detail)
			}
		}
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
