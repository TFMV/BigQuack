package bigquack_test

import (
	"context"
	"fmt"
	"testing"

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
	})
	if err != nil {
		t.Fatalf("failed to BQ2Duck: %v", err)
	}
	fmt.Println(rows)
}
