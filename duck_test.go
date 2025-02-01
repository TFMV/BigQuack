package bigquack_test

import (
	"context"
	"fmt"
	"testing"

	bigquack "github.com/TFMV/BigQuack"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDuckDB(t *testing.T) {
	// Attempt creating an in-memory DuckDB
	duck, err := bigquack.NewDuckDB(
		bigquack.WithPath(""), // empty => in-memory
	)
	require.NoError(t, err, "creating DuckDB in-memory should not fail")
	assert.NotNil(t, duck)

	// Check default state
	assert.Equal(t, "", duck.Path(), "path should be empty for in-memory DB")
	assert.Equal(t, 0, duck.ConnCount(), "no open connections yet")

	// Cleanup
	duck.Close()
}

func TestOpenConnection(t *testing.T) {
	duck, err := bigquack.NewDuckDB(
		bigquack.WithPath(""),
	)
	require.NoError(t, err)
	defer duck.Close()

	// 1) Open a connection
	conn, err := duck.OpenConnection()
	require.NoError(t, err, "opening connection should succeed")
	require.NotNil(t, conn, "connection should not be nil")
	assert.Equal(t, 1, duck.ConnCount(), "should have 1 open connection")

	// 2) Close the connection
	conn.Close()
	assert.Equal(t, 0, duck.ConnCount(), "connection should be removed from the parent")
}

func TestExecAndQuery(t *testing.T) {
	duck, err := bigquack.NewDuckDB()
	require.NoError(t, err)
	defer duck.Close()

	conn, err := duck.OpenConnection()
	require.NoError(t, err)
	defer conn.Close()

	// Create a table
	createSQL := "CREATE TABLE people (id INT, name STRING, score DOUBLE);"
	rowsAffected, err := conn.Exec(context.Background(), createSQL)
	require.NoError(t, err, "exec create table")
	// For DDL, we typically get -1 as no row count
	t.Logf("create table => rows=%d", rowsAffected)

	// Insert data
	insertSQL := `
    INSERT INTO people VALUES
      (1, 'Alice', 95.5),
      (2, 'Bob',   87.2),
      (3, 'Eve',   78.9);
    `
	insCount, err := conn.Exec(context.Background(), insertSQL)
	require.NoError(t, err, "insert rows")
	t.Logf("insert => rows=%d", insCount)
	// Usually -1 for DuckDB

	// Query the data
	sql := `SELECT id, name, score FROM people ORDER BY id`
	rr, stmt, rowCount, err := conn.Query(context.Background(), sql)
	require.NoError(t, err, "query should succeed")
	require.NotNil(t, rr)
	require.NotNil(t, stmt)
	t.Logf("rowCount => %d", rowCount)
	defer stmt.Close()
	defer rr.Release()

	var recs []arrow.Record
	for rr.Next() {
		rec := rr.Record()
		rec.Retain()
		recs = append(recs, rec)
	}
	require.NoError(t, rr.Err(), "no iteration error expected")
	// We'll combine them if multiple
	// But typically it's just one chunk from DuckDB
	if len(recs) == 0 {
		t.Fatal("expected at least one record batch from query")
	}
	// Typically 1 record
	var totalRows int64
	for _, r := range recs {
		totalRows += int64(r.NumRows())
	}
	assert.EqualValues(t, 3, totalRows, "expect 3 rows total")

	// Check the data
	batch := recs[0]
	assert.EqualValues(t, 3, batch.NumRows(), "expect 3 rows")
	assert.EqualValues(t, 3, batch.NumCols(), "expect 3 columns")

	colID := batch.Column(0).(*array.Int32)
	colName := batch.Column(1).(*array.String)
	colScore := batch.Column(2).(*array.Float64)

	// row 0
	assert.EqualValues(t, 1, colID.Value(0))
	assert.Equal(t, "Alice", colName.Value(0))
	assert.InDelta(t, 95.5, colScore.Value(0), 0.001)

	// row 1
	assert.EqualValues(t, 2, colID.Value(1))
	assert.Equal(t, "Bob", colName.Value(1))
	// row 2
	// ...
}

func TestIngestCreateAppend(t *testing.T) {
	duck, err := bigquack.NewDuckDB()
	require.NoError(t, err)
	defer duck.Close()

	conn, err := duck.OpenConnection()
	require.NoError(t, err)
	defer conn.Close()

	// We'll create an Arrow record to ingest
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "uid", Type: arrow.PrimitiveTypes.Int64},
		{Name: "uname", Type: arrow.BinaryTypes.String},
	}, nil)

	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Build data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 11}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Kiki", "Lala"}, nil)

	rec := builder.NewRecord()
	defer rec.Release()

	// Ingest => table doesn't exist, so mode=Create
	tableName := "users"
	rows, err := conn.IngestCreateAppend(context.Background(), tableName, rec)
	require.NoError(t, err, "ingest create")
	// Typically -1 for row count in DuckDB
	t.Logf("ingest create => rows=%d", rows)

	// Let's ingest again => mode=Append
	rows2, err := conn.IngestCreateAppend(context.Background(), tableName, rec)
	require.NoError(t, err, "ingest append")
	t.Logf("ingest append => rows=%d", rows2)

	// We expect the table now has 4 total rows
	sql := fmt.Sprintf("SELECT uid, uname FROM %s ORDER BY uid", tableName)
	rr, stmt, _, err := conn.Query(context.Background(), sql)
	require.NoError(t, err)
	defer rr.Release()
	defer stmt.Close()

	var recs []arrow.Record
	for rr.Next() {
		r := rr.Record()
		r.Retain()
		recs = append(recs, r)
	}
	require.NoError(t, rr.Err())

	var total int64
	for _, r := range recs {
		total += int64(r.NumRows())
	}
	assert.EqualValues(t, 4, total, "2 from first ingest + 2 from second = 4")
}
