// Package bigquack integrates DuckDB (via the Arrow ADBC driver)
// with BigQuery (via the BigQuery Storage API), using Apache Arrow
// for high-performance data exchange.
package bigquack

import (
	"context"
	"io"
	"time"

	"go.uber.org/zap"
)

// Package-level zap logger
var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic("failed to initialize zap logger: " + err.Error())
	}
}

// BigQuack is a top-level struct that orchestrates connections to DuckDB and BigQuery.
type BigQuack struct {
	duckDB   *DuckDB
	bigQuery *BigQueryReadClient
}

// NewBigQuack initializes both DuckDB and the BigQuery Read Client.
func NewBigQuack() (*BigQuack, error) {
	// Create DuckDB
	duckDB, err := NewDuckDB()
	if err != nil {
		logger.Error("failed to create DuckDB", zap.Error(err))
		return nil, err
	}
	logger.Info("DuckDB created successfully")

	// Create BigQuery read client
	bigQuery, err := NewBigQueryReadClient(context.Background())
	if err != nil {
		logger.Error("failed to create BigQuery read client", zap.Error(err))
		// If we already created duckDB but BigQuery fails, close duckDB
		duckDB.Close()
		return nil, err
	}
	logger.Info("BigQuery read client created successfully")

	return &BigQuack{
		duckDB:   duckDB,
		bigQuery: bigQuery,
	}, nil
}

func (bq *BigQuack) Close() error {
	logger.Info("closing BigQuack", zap.String("component", "BigQuack"))
	bq.duckDB.Close()
	return nil
}

// BQ2Duck reads *all* data from BigQuery into DuckDB, returning the total row count
func (bq *BigQuack) BQ2Duck(
	ctx context.Context,
	duckDBPath string,
	projectID, datasetID, tableID string,
	opts *BigQueryReaderOptions,
) (int64, error) {

	startTime := time.Now()
	logger.Info("starting BQ2Duck",
		zap.String("projectID", projectID),
		zap.String("datasetID", datasetID),
		zap.String("tableID", tableID))

	// 1) Open DuckDB connection
	conn, err := bq.duckDB.OpenConnection()
	if err != nil {
		logger.Error("failed to open DuckDB connection", zap.Error(err))
		return 0, err
	}
	defer conn.Close()

	// 2) Fetch table data from BigQuery (Arrow format)
	reader, err := bq.bigQuery.NewBigQueryReader(ctx, projectID, datasetID, tableID, opts)
	if err != nil {
		logger.Error("failed to create BigQuery reader",
			zap.String("table", tableID),
			zap.Error(err),
		)
		return 0, err
	}
	defer reader.Close()

	var totalIngestedRows int64

	// 3) Continuously read from BigQuery until io.EOF
	for {
		rec, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				// no more data from BigQuery
				break
			}
			logger.Error("error reading from BigQuery",
				zap.String("table", tableID),
				zap.Error(err),
			)
			return totalIngestedRows, err
		}

		rowCount := int64(rec.NumRows())
		if rowCount == 0 {
			rec.Release()
			continue
		}

		// 4) Ingest record into DuckDB
		if _, ingestErr := conn.IngestCreateAppend(ctx, tableID, rec); ingestErr != nil {
			logger.Error("failed to ingest into DuckDB table", zap.String("table", tableID), zap.Error(ingestErr))
			rec.Release()
			return totalIngestedRows, ingestErr
		}
		rec.Release()

		// Accumulate row count
		totalIngestedRows += rowCount
		logger.Info("ingested batch",
			zap.Int64("numRowsInRecord", rowCount),
			zap.Int64("totalSoFar", totalIngestedRows),
		)
	}

	elapsed := time.Since(startTime)
	logger.Info("BQ2Duck completed",
		zap.Int64("totalIngestedRows", totalIngestedRows),
		zap.Duration("duration", elapsed))

	return totalIngestedRows, nil
}
