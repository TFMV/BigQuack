# BigQuack

BigQuack is a Go solution that facilitates a high-performance pipeline between Google BigQuery and DuckDB, leveraging the Apache Arrow ecosystem.

## Under the Hood

- BigQuack uses the [Arrow ADBC](https://arrow.apache.org/docs/go/adbc/) interface to interact with DuckDB.
- BigQuack uses the [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage) to interact with BigQuery.

## Usage

```go
// Create a new BigQuack instance
bq, err := bigquack.NewBigQuack()
if err != nil {
    t.Fatalf("failed to create BigQuack: %v", err)
}
defer bq.Close()

// Ingest data from BigQuery to DuckDB
rows, err := bq.BQ2Duck(context.Background(), "test.db", "tfmv-371720", "tpch", "supplier", &bigquack.BigQueryReaderOptions{
    MaxStreamCount: 2,
    TableReadOptions: &storagepb.ReadSession_TableReadOptions{
        SelectedFields: []string{"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"},
        RowRestriction: "s_suppkey > 10",
    },
})
if err != nil {
    t.Fatalf("failed to BQ2Duck: %v", err)
}
fmt.Println(rows)
```

## Author

[Thomas F McGeehan V](https://github.com/TFMV)

## License

MIT License - see the [LICENSE file](LICENSE) for details
