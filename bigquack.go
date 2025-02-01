package bigquack

import (
	"context"
)

type BigQuack struct {
	duckDB   *DuckDB
	bigQuery *BigQueryReadClient
}

func NewBigQuack() (*BigQuack, error) {
	duckDB, err := NewDuckDB()
	if err != nil {
		return nil, err
	}
	bigQuery, err := NewBigQueryReadClient(context.Background())
	if err != nil {
		return nil, err
	}
	return &BigQuack{
		duckDB:   duckDB,
		bigQuery: bigQuery,
	}, nil
}

func (bq *BigQuack) Close() error {
	bq.duckDB.Close()
	return nil
}

func (bq *BigQuack) BQ2Duck(ctx context.Context, duckDBPath string, projectID string, datasetID string, tableID string, opts *BigQueryReaderOptions) (int64, error) {
	conn, err := bq.duckDB.OpenConnection()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Fetch table data from BigQuery and ingest into DuckDB
	reader, err := bq.bigQuery.NewBigQueryReader(context.Background(), projectID, datasetID, tableID, opts)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	rec, err := reader.Read()
	if err != nil {
		return 0, err
	}
	conn.IngestCreateAppend(context.Background(), "supplier", rec)

	// Query DuckDB and print results
	rows, _, _, err := conn.Query(context.Background(), "SELECT * FROM supplier")
	if err != nil {
		return 0, err
	}
	defer rows.Release()

	var totalRows int64
	for rows.Next() {
		rec := rows.Record()
		totalRows += int64(rec.NumRows())
	}
	return totalRows, nil
}
