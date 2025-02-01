package bigquack_test

import (
	"context"
	"fmt"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	bigquack "github.com/TFMV/BigQuack"
)

func TestBQ2Duck(t *testing.T) {
	t.Parallel()
	bq, err := bigquack.NewBigQuack()
	if err != nil {
		t.Fatalf("failed to create BigQuack: %v", err)
	}
	defer bq.Close()
	rows, err := bq.BQ2Duck(context.Background(), "test.db", "tfmv-371720", "tpch", "supplier", &bigquack.BigQueryReaderOptions{
		MaxStreamCount: 2,
		TableReadOptions: &storagepb.ReadSession_TableReadOptions{
			SelectedFields: []string{"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"},
			RowRestriction: "s_suppkey > 0",
		},
	})
	if err != nil {
		t.Fatalf("failed to BQ2Duck: %v", err)
	}
	fmt.Println(rows)
}
