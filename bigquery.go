package bigquack

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

// BigQueryReadClient wraps a BigQuery Storage client for reading Arrow-serialized data
// from BigQuery tables.
type BigQueryReadClient struct {
	client      *bqStorage.BigQueryReadClient
	callOptions *BigQueryReadCallOptions
}

// BigQueryReadCallOptions stores gax.CallOption slices for CreateReadSession and ReadRows RPCs.
// You can modify or override these to handle custom timeouts, retries, etc.
type BigQueryReadCallOptions struct {
	CreateReadSession []gax.CallOption
	ReadRows          []gax.CallOption
}

// defaultBigQueryReadCallOptions sets baseline timeouts and retries for BigQuery read ops.
func defaultBigQueryReadCallOptions() *BigQueryReadCallOptions {
	return &BigQueryReadCallOptions{
		CreateReadSession: []gax.CallOption{
			gax.WithTimeout(600 * time.Second),
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60 * time.Second,
					Multiplier: 1.30,
				})
			}),
		},
		ReadRows: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60 * time.Second,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// NewBigQueryReadClient constructs a BigQuery Storage client for reading Arrow data.
// Provide `option.ClientOption` if you need to specify credentials or scopes.
func NewBigQueryReadClient(ctx context.Context, opts ...option.ClientOption) (*BigQueryReadClient, error) {
	client, err := bqStorage.NewBigQueryReadClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryReadClient: %w", err)
	}

	return &BigQueryReadClient{
		client:      client,
		callOptions: defaultBigQueryReadCallOptions(),
	}, nil
}

type BigQueryReaderOptions struct {
	MaxStreamCount   int32
	TableReadOptions *storagepb.ReadSession_TableReadOptions
}

// NewBigQueryReader creates a new reader for the specified table.
// If opts is nil, default options will be used.
func (c *BigQueryReadClient) NewBigQueryReader(ctx context.Context, project, dataset, table string, opts *BigQueryReaderOptions) (*BigQueryReader, error) {
	req := &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", project),
		ReadSession: &storagepb.ReadSession{
			Table:       fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table),
			DataFormat:  storagepb.DataFormat_ARROW,
			ReadOptions: opts.TableReadOptions,
		},
		MaxStreamCount: 1,
	}

	session, err := c.client.CreateReadSession(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create read session: %w", err)
	}
	if len(session.GetStreams()) == 0 {
		return nil, fmt.Errorf("no streams available in session for table %s", table)
	}

	alloc := memory.NewGoAllocator()
	schemaBytes := session.GetArrowSchema().GetSerializedSchema()
	if len(schemaBytes) == 0 {
		return nil, fmt.Errorf("could not retrieve Arrow schema from BigQuery")
	}

	// Initialize an IPC reader solely to parse the schema
	buf := bytes.NewBuffer(schemaBytes)
	ipcReader, err := ipc.NewReader(buf, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("failed to parse Arrow schema from BigQuery: %w", err)
	}

	r := &BigQueryReader{
		ctx:         ctx,
		client:      c.client,
		callOptions: c.callOptions,
		schemaBytes: schemaBytes,
		streams:     session.GetStreams(),
		mem:         alloc,
		buf:         bytes.NewBuffer(nil),
		r:           ipcReader,
	}

	return r, nil
}

// BigQueryReader reads Arrow records from a BigQuery Storage read session.
// Use Read() to iterate over rows. Close() when done to free resources.
type BigQueryReader struct {
	ctx         context.Context
	client      *bqStorage.BigQueryReadClient
	callOptions *BigQueryReadCallOptions
	schemaBytes []byte
	streams     []*storagepb.ReadStream

	// For reading data
	mem    memory.Allocator
	stream storagepb.BigQueryRead_ReadRowsClient
	offset int64

	// Reusable buffers
	r   *ipc.Reader
	buf *bytes.Buffer
}

// Read fetches the next Arrow record from BigQuery. Returns io.EOF if there are
// no more records. Each record must be released after usage to avoid memory leaks.
func (r *BigQueryReader) Read() (arrow.Record, error) {
	for {
		// If there's a current IPC reader with unconsumed records
		if r.r != nil && r.r.Next() {
			rec := r.r.Record()
			rec.Retain()
			return rec, nil
		}

		// No more unconsumed records => fetch next arrow batch from BQ
		resp, err := r.readNextResponse()
		if err != nil {
			return nil, err
		}

		batch := resp.GetArrowRecordBatch().GetSerializedRecordBatch()
		if len(batch) == 0 {
			// This batch is empty, or no data => keep going
			continue
		}
		// Construct an arrow record from the batch
		rec, err := r.processRecordBatch(batch)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			return rec, nil
		}
		// else loop for next response
	}
}

// readNextResponse pulls the next chunk of rows from the stream or starts a new stream if needed.
func (r *BigQueryReader) readNextResponse() (*storagepb.ReadRowsResponse, error) {
	if r.stream == nil {
		if len(r.streams) == 0 {
			return nil, io.EOF
		}
		// Start reading from the first (only) stream
		streamName := r.streams[0].GetName()
		newStream, err := r.client.ReadRows(r.ctx, &storagepb.ReadRowsRequest{
			ReadStream: streamName,
			Offset:     r.offset,
		}, r.callOptions.ReadRows...)
		if err != nil {
			return nil, fmt.Errorf("failed to open ReadRows stream: %w", err)
		}
		r.stream = newStream
	}

	response, err := r.stream.Recv()
	if err == io.EOF {
		r.stream = nil
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("error receiving BigQuery stream data: %w", err)
	}
	r.offset += response.GetRowCount()
	return response, nil
}

// processRecordBatch merges schema + batch, reinitializes the IPC reader to parse it.
func (r *BigQueryReader) processRecordBatch(data []byte) (arrow.Record, error) {
	// Wipe old buffer, re-inject schema + batch
	r.buf.Reset()
	r.buf.Write(r.schemaBytes)
	r.buf.Write(data)

	// We reuse the existing schema from r.r.Schema() if needed:
	schema := r.r.Schema()
	var err error
	r.r, err = ipc.NewReader(r.buf, ipc.WithAllocator(r.mem), ipc.WithSchema(schema))
	if err != nil {
		return nil, fmt.Errorf("failed to create new IPC reader for batch: %w", err)
	}

	if r.r.Next() {
		rec := r.r.Record()
		rec.Retain()
		return rec, nil
	}
	if e := r.r.Err(); e != nil && e != io.EOF {
		return nil, fmt.Errorf("arrow IPC read error: %w", e)
	}
	// No record found in this batch => no error, just no data
	return nil, nil
}

// Schema retrieves the Arrow schema from the BQ read session. Must be called
// after initialization. Returns an error if the schema is not ready.
func (r *BigQueryReader) Schema() (*arrow.Schema, error) {
	if r.r == nil {
		return nil, fmt.Errorf("no schema or IPC reader available")
	}
	return r.r.Schema(), nil
}

// Close cleans up resources used by the BigQueryReader. Safe to call multiple times.
func (r *BigQueryReader) Close() error {
	if r.r != nil {
		r.r.Release()
		r.r = nil
	}
	// We don't explicitly close the gRPC stream. No official method in generated stubs.
	// It's sufficient to discard the client or let the context expire.
	return nil
}
