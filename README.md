# BigQuack

BigQuack is a Go solution that facilitates a high-performance pipeline between Google BigQuery and DuckDB, leveraging the Apache Arrow ecosystem.

*The DuckDB implementation is draws from the [loicalleyne/couac](https://github.com/loicalleyne/couac) implementation.*

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
rows, err := bq.BQ2Duck(context.Background(), "test.db", "tfmv-371720", "tpch", "supplier", nil)
if err != nil {
    t.Fatalf("failed to BQ2Duck: %v", err)
}
fmt.Println(rows)
```

## Author

[Thomas F McGeehan V](https://github.com/TFMV)

## License

MIT License - see the [LICENSE file](LICENSE) for details
