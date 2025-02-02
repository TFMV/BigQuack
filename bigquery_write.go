package bigquack

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	"github.com/TFMV/arrowpb"
)

// -----------------------------------------------------------------------------
// BigQueryWriteClient (existing support)
// -----------------------------------------------------------------------------

// BigQueryWriteClient wraps a BigQuery Storage Write API client for writing Arrow data.
type BigQueryWriteClient struct {
	client      *bqStorage.BigQueryWriteClient
	schema      *arrow.Schema
	callOptions *BigQueryWriteCallOptions
}

// BigQueryWriteCallOptions stores gax.CallOption slices for the AppendRows RPC.
type BigQueryWriteCallOptions struct {
	AppendRows []gax.CallOption
}

func defaultBigQueryWriteCallOptions() *BigQueryWriteCallOptions {
	return &BigQueryWriteCallOptions{
		AppendRows: []gax.CallOption{
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
	}
}

// NewBigQueryWriteClient creates a new BigQueryWriteClient.
// serviceAccountJSON may be a file path or a raw JSON string.
func NewBigQueryWriteClient(ctx context.Context, serviceAccountJSON string, schema *arrow.Schema, opts ...option.ClientOption) (*BigQueryWriteClient, error) {
	// If serviceAccountJSON is a path, read its contents.
	if _, err := os.Stat(serviceAccountJSON); err == nil {
		content, err := os.ReadFile(serviceAccountJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to read service account JSON file: %w", err)
		}
		serviceAccountJSON = string(content)
	}
	client, err := bqStorage.NewBigQueryWriteClient(ctx, option.WithCredentialsJSON([]byte(serviceAccountJSON)))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery Storage Write client: %w", err)
	}
	return &BigQueryWriteClient{
		client:      client,
		schema:      schema,
		callOptions: defaultBigQueryWriteCallOptions(),
	}, nil
}

// -----------------------------------------------------------------------------
// BigQueryWriteOptions
// -----------------------------------------------------------------------------

// BigQueryWriteOptions allows customization of the write stream.
type BigQueryWriteOptions struct {
	WriteStreamType storagepb.WriteStream_Type
	Allocator       memory.Allocator
	DataFormat      storagepb.DataFormat
}

func NewDefaultBigQueryWriteOptions() *BigQueryWriteOptions {
	return &BigQueryWriteOptions{
		WriteStreamType: storagepb.WriteStream_COMMITTED,
		Allocator:       memory.NewGoAllocator(),
		DataFormat:      storagepb.DataFormat_ARROW,
	}
}

// -----------------------------------------------------------------------------
// BigQueryManagedRecordWriter using the Managed Writer API
// -----------------------------------------------------------------------------

// BigQueryManagedRecordWriter writes Arrow records to BigQuery using the managed writer.
type BigQueryManagedRecordWriter struct {
	ctx           context.Context
	writeClient   *bqStorage.BigQueryWriteClient
	managedClient *managedwriter.Client
	managedStream *managedwriter.ManagedStream

	arrowSchema *arrow.Schema
	offset      int64

	mem    memory.Allocator
	buffer *bytes.Buffer
	mu     sync.Mutex
}

// NewBigQueryManagedRecordWriter creates a new managed record writer.
// It establishes a pending write stream, converts the Arrow schema into a normalized
// descriptor, and then creates a ManagedStream for appending rows.
func NewBigQueryManagedRecordWriter(ctx context.Context, serviceAccountJSON string, schema *arrow.Schema, project, dataset, table string, opts *BigQueryWriteOptions, clientOpts ...option.ClientOption) (*BigQueryManagedRecordWriter, error) {
	// Create underlying BigQueryWriteClient.
	bqWriteClient, err := NewBigQueryWriteClient(ctx, serviceAccountJSON, schema, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryWriteClient: %w", err)
	}

	// Create a managed writer client.
	mClient, err := managedwriter.NewClient(ctx, project, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create managed writer client: %w", err)
	}

	// Define the table resource name.
	parent := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table)

	// Create a pending write stream with DataFormat set to ARROW.
	pendingStream, err := bqWriteClient.client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: parent,
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_PENDING,
		},
	}, bqWriteClient.callOptions.AppendRows...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pending write stream: %w", err)
	}

	// Use a conversion config that disables wrapper types so that the proto schema matches BigQuery table.
	convertCfg := &arrowpb.ConvertConfig{
		UseWrapperTypes:        false,
		UseWellKnownTimestamps: false,
	}

	// Convert Arrow schema to a FileDescriptorProto.
	fdp, err := arrowpb.ArrowSchemaToFileDescriptorProto(schema, "bigquack", "ManagedMsg", convertCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Arrow schema to FileDescriptorProto: %w", err)
	}
	// Compile the descriptor and extract the top-level message.
	fdProto, err := arrowpb.CompileFileDescriptorProtoWithRetry(fdp)
	if err != nil {
		return nil, fmt.Errorf("failed to compile FileDescriptorProto: %w", err)
	}
	msgDesc, err := arrowpb.GetTopLevelMessageDescriptor(fdProto)
	if err != nil {
		return nil, fmt.Errorf("failed to get top-level message descriptor: %w", err)
	}
	// Normalize the descriptor to obtain a self-contained schema descriptor.
	normalizedDesc, err := adapt.NormalizeDescriptor(msgDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize descriptor: %w", err)
	}

	// Create a ManagedStream.
	managedStream, err := mClient.NewManagedStream(ctx,
		managedwriter.WithStreamName(pendingStream.GetName()),
		managedwriter.WithSchemaDescriptor(normalizedDesc),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create managed stream: %w", err)
	}

	// Initialize an IPC writer (the buffer will be reset for each record write).
	alloc := memory.NewGoAllocator()
	buf := &bytes.Buffer{}

	// Use the provided allocator if opts is set; otherwise default.
	var writerAlloc memory.Allocator
	if opts != nil && opts.Allocator != nil {
		writerAlloc = opts.Allocator
	} else {
		writerAlloc = alloc
	}

	return &BigQueryManagedRecordWriter{
		ctx:           ctx,
		writeClient:   bqWriteClient.client,
		managedClient: mClient,
		managedStream: managedStream,
		arrowSchema:   schema,
		offset:        0,
		mem:           writerAlloc,
		buffer:        buf,
	}, nil
}

// WriteRecord serializes an Arrow record to IPC format and appends it via the managed stream.
func (w *BigQueryManagedRecordWriter) WriteRecord(rec arrow.Record) error {
	if !w.arrowSchema.Equal(rec.Schema()) {
		return fmt.Errorf("schema mismatch: expected %v but got %v", w.arrowSchema, rec.Schema())
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer.Reset()
	// Create an IPC writer with options (e.g. LZ4 compression, dictionary deltas)
	ipcW := ipc.NewWriter(w.buffer,
		ipc.WithSchema(w.arrowSchema),
		ipc.WithAllocator(w.mem),
		ipc.WithLZ4(),
		ipc.WithDictionaryDeltas(true),
	)
	if err := ipcW.Write(rec); err != nil {
		return fmt.Errorf("failed to write record via IPC: %w", err)
	}
	if err := ipcW.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}

	serialized := w.buffer.Bytes()
	if len(serialized) == 0 {
		return fmt.Errorf("serialized record is empty")
	}

	// Append the serialized Arrow record batch via the managed stream.
	result, err := w.managedStream.AppendRows(w.ctx, [][]byte{serialized},
		managedwriter.WithOffset(w.offset),
	)
	if err != nil {
		return fmt.Errorf("failed to append rows: %w", err)
	}
	recvOffset, err := result.GetResult(w.ctx)
	if err != nil {
		// If the error includes row-level details, they may be found in the row_errors field.
		return fmt.Errorf("failed to get append result: %w", err)
	}
	// Update the offset (each record counts as one row for our simple example).
	w.offset = recvOffset + 1
	return nil
}

// Finalize finalizes the managed stream and commits the data to BigQuery.
func (w *BigQueryManagedRecordWriter) Finalize() (int64, error) {
	// Finalize the stream; this blocks further appends.
	rowCount, err := w.managedStream.Finalize(w.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to finalize stream: %w", err)
	}
	// Batch commit the stream.
	commitReq := &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       managedwriter.TableParentFromStreamName(w.managedStream.StreamName()),
		WriteStreams: []string{w.managedStream.StreamName()},
	}
	commitResp, err := w.managedClient.BatchCommitWriteStreams(w.ctx, commitReq)
	if err != nil {
		return 0, fmt.Errorf("failed to batch commit write streams: %w", err)
	}
	if len(commitResp.GetStreamErrors()) > 0 {
		return 0, fmt.Errorf("stream errors: %v", commitResp.GetStreamErrors())
	}
	return rowCount, nil
}

// Close closes the managed stream and releases associated resources.
func (w *BigQueryManagedRecordWriter) Close() error {
	return w.managedStream.Close()
}
