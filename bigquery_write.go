// bigquery_write.go
//
// Production-ready implementation for writing Arrow records to BigQuery using the
// managed writer API. It includes improved error handling, schema conversion from
// BigQuery table metadata, and configurable options for the write stream.
//
// Note: The conversion from Storage TableSchema to a Protobuf DescriptorProto here
// is a minimal implementation covering only basic (non-nested) fields.
// For production use, you may wish to extend this to support nested/complex types.

package bigquack

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ----------------------------------------------------------------------------
// BigQueryWriteClient and call options
// ----------------------------------------------------------------------------

// BigQueryWriteCallOptions stores gax.CallOption slices for the AppendRows RPC.
type BigQueryWriteCallOptions struct {
	AppendRows []gax.CallOption
}

// defaultBigQueryWriteCallOptions returns the default call options.
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

// BigQueryWriteClient wraps a BigQuery Storage Write API client.
type BigQueryWriteClient struct {
	client      *bqStorage.BigQueryWriteClient
	schema      *arrow.Schema
	callOptions *BigQueryWriteCallOptions
}

// NewBigQueryWriteClient creates a new BigQueryWriteClient.
// The serviceAccountJSON may be either a file path or a raw JSON string.
func NewBigQueryWriteClient(ctx context.Context, serviceAccountJSON string, schema *arrow.Schema, opts ...option.ClientOption) (*BigQueryWriteClient, error) {
	// If serviceAccountJSON is a file path, read its contents.
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

// Close releases resources associated with the BigQueryWriteClient.
func (c *BigQueryWriteClient) Close() error {
	return c.client.Close()
}

// ----------------------------------------------------------------------------
// BigQueryWriteOptions
// ----------------------------------------------------------------------------

// BigQueryWriteOptions allows customization of the write stream.
type BigQueryWriteOptions struct {
	// WriteStreamType determines the type of stream to create.
	// For managed writer usage it is common to use a PENDING stream.
	// (Note: if you choose a COMMITTED stream, then the stream is immediately visible.)
	WriteStreamType storagepb.WriteStream_Type

	// Allocator for Arrow buffers. If nil, a default Go allocator is used.
	Allocator memory.Allocator

	// DataFormat for the write stream (e.g. ARROW).
	DataFormat storagepb.DataFormat

	// UseWrapperTypes indicates whether the generated descriptor should use wrapper
	// types (for example, a STRING field becomes a message field of type
	// .google.protobuf.StringValue). This should match how your Arrow schema is converted.
	UseWrapperTypes bool
}

// NewDefaultBigQueryWriteOptions returns default options suitable for managed writer.
func NewDefaultBigQueryWriteOptions() *BigQueryWriteOptions {
	return &BigQueryWriteOptions{
		WriteStreamType: storagepb.WriteStream_PENDING, // use PENDING so Finalize/Commit is required
		Allocator:       memory.NewGoAllocator(),
		DataFormat:      storagepb.DataFormat_ARROW,
		UseWrapperTypes: true,
	}
}

// ----------------------------------------------------------------------------
// Schema conversion helpers
// ----------------------------------------------------------------------------

// BQSchemaToStorageTableSchema converts a BigQuery Schema into a Storage TableSchema.
func BQSchemaToStorageTableSchema(in bigquery.Schema) (*storagepb.TableSchema, error) {
	if in == nil {
		return nil, nil
	}
	out := &storagepb.TableSchema{}
	for _, s := range in {
		converted, err := bqFieldToProto(s)
		if err != nil {
			return nil, err
		}
		out.Fields = append(out.Fields, converted)
	}
	return out, nil
}

func bqFieldToProto(in *bigquery.FieldSchema) (*storagepb.TableFieldSchema, error) {
	if in == nil {
		return nil, nil
	}
	out := &storagepb.TableFieldSchema{
		Name:        in.Name,
		Description: in.Description,
	}

	// Map BigQuery field types to Storage API types.
	switch in.Type {
	case bigquery.StringFieldType:
		out.Type = storagepb.TableFieldSchema_STRING
	case bigquery.BytesFieldType:
		out.Type = storagepb.TableFieldSchema_BYTES
	case bigquery.IntegerFieldType:
		out.Type = storagepb.TableFieldSchema_INT64
	case bigquery.FloatFieldType:
		out.Type = storagepb.TableFieldSchema_DOUBLE
	case bigquery.BooleanFieldType:
		out.Type = storagepb.TableFieldSchema_BOOL
	case bigquery.TimestampFieldType:
		out.Type = storagepb.TableFieldSchema_TIMESTAMP
	case bigquery.RecordFieldType:
		out.Type = storagepb.TableFieldSchema_STRUCT
	case bigquery.DateFieldType:
		out.Type = storagepb.TableFieldSchema_DATE
	case bigquery.TimeFieldType:
		out.Type = storagepb.TableFieldSchema_TIME
	case bigquery.DateTimeFieldType:
		out.Type = storagepb.TableFieldSchema_DATETIME
	case bigquery.NumericFieldType:
		out.Type = storagepb.TableFieldSchema_NUMERIC
	case bigquery.BigNumericFieldType:
		out.Type = storagepb.TableFieldSchema_BIGNUMERIC
	case bigquery.GeographyFieldType:
		out.Type = storagepb.TableFieldSchema_GEOGRAPHY
	case bigquery.RangeFieldType:
		out.Type = storagepb.TableFieldSchema_RANGE
	case bigquery.JSONFieldType:
		out.Type = storagepb.TableFieldSchema_JSON
	default:
		return nil, fmt.Errorf("could not convert field (%s) due to unknown type value: %s", in.Name, in.Type)
	}

	// Mode conversion.
	if in.Repeated {
		out.Mode = storagepb.TableFieldSchema_REPEATED
	} else if in.Required {
		out.Mode = storagepb.TableFieldSchema_REQUIRED
	} else {
		out.Mode = storagepb.TableFieldSchema_NULLABLE
	}

	// Process nested schema if present.
	for _, s := range in.Schema {
		subField, err := bqFieldToProto(s)
		if err != nil {
			return nil, err
		}
		out.Fields = append(out.Fields, subField)
	}
	return out, nil
}

// convertTableSchemaToDescriptor converts a Storage API TableSchema into a minimal
// DescriptorProto. This implementation only handles top-level (non-nested) fields.
// The useWrapperTypes flag controls whether STRING fields are emitted as a message
// field using .google.protobuf.StringValue.
func convertTableSchemaToDescriptor(schema *storagepb.TableSchema, useWrapperTypes bool) (*descriptorpb.DescriptorProto, error) {
	// Create a new descriptor with the name "Row"
	desc := &descriptorpb.DescriptorProto{
		Name: proto.String("Row"),
		// No custom options are set here.
	}
	var fields []*descriptorpb.FieldDescriptorProto
	for i, f := range schema.Fields {
		fd, err := convertFieldSchemaToFieldDescriptor(f, int32(i+1), useWrapperTypes)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %q: %w", f.Name, err)
		}
		fields = append(fields, fd)
	}
	desc.Field = fields
	return desc, nil
}

// convertFieldSchemaToFieldDescriptor converts a Storage API TableFieldSchema into a FieldDescriptorProto.
// The useWrapperTypes flag controls whether STRING fields are emitted as a message
// field using .google.protobuf.StringValue.
func convertFieldSchemaToFieldDescriptor(f *storagepb.TableFieldSchema, number int32, useWrapperTypes bool) (*descriptorpb.FieldDescriptorProto, error) {
	var fieldType descriptorpb.FieldDescriptorProto_Type
	var typeName *string

	switch f.Type {
	case storagepb.TableFieldSchema_STRING:
		if useWrapperTypes {
			// Use the wrapper message type for strings.
			fieldType = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
			typeName = proto.String(".google.protobuf.StringValue")
		} else {
			fieldType = descriptorpb.FieldDescriptorProto_TYPE_STRING
		}
	case storagepb.TableFieldSchema_BYTES:
		fieldType = descriptorpb.FieldDescriptorProto_TYPE_BYTES
	case storagepb.TableFieldSchema_INT64:
		fieldType = descriptorpb.FieldDescriptorProto_TYPE_INT64
	case storagepb.TableFieldSchema_DOUBLE:
		fieldType = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case storagepb.TableFieldSchema_BOOL:
		fieldType = descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case storagepb.TableFieldSchema_TIMESTAMP:
		// Represent timestamp as int64.
		fieldType = descriptorpb.FieldDescriptorProto_TYPE_INT64
	default:
		return nil, fmt.Errorf("unsupported field type: %v", f.Type)
	}

	// Map mode to label.
	var label descriptorpb.FieldDescriptorProto_Label
	switch f.Mode {
	case storagepb.TableFieldSchema_REQUIRED:
		label = descriptorpb.FieldDescriptorProto_LABEL_REQUIRED
	case storagepb.TableFieldSchema_REPEATED:
		label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	default:
		label = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	}

	// This minimal conversion does not support nested fields.
	if len(f.Fields) > 0 {
		return nil, fmt.Errorf("nested fields are not supported for field: %s", f.Name)
	}

	fd := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(f.Name),
		Number: proto.Int32(number),
		Label:  &label,
		Type:   &fieldType,
	}
	if typeName != nil {
		fd.TypeName = typeName
	}
	return fd, nil
}

// ----------------------------------------------------------------------------
// BigQueryManagedRecordWriter
// ----------------------------------------------------------------------------

// BigQueryManagedRecordWriter writes Arrow records to BigQuery using the managed writer API.
type BigQueryManagedRecordWriter struct {
	// ctx is the caller-supplied context.
	ctx context.Context
	// writeClient is the underlying BigQuery Storage Write client.
	writeClient *bqStorage.BigQueryWriteClient
	// managedClient is the managed writer client.
	managedClient *managedwriter.Client
	// managedStream is the active managed stream for appending rows.
	managedStream *managedwriter.ManagedStream

	// arrowSchema is the expected schema for Arrow records.
	arrowSchema *arrow.Schema
	// offset tracks the next row offset.
	offset int64

	// mem is the memory allocator for Arrow buffers.
	mem memory.Allocator
	// buffer is used for serializing records.
	buffer *bytes.Buffer
	// mu protects buffer usage.
	mu sync.Mutex
}

// NewBigQueryManagedRecordWriter creates a new managed record writer.
// It creates a BigQuery Storage Write client, retrieves table metadata from BigQuery,
// converts the table schema, creates a pending write stream, and then a managed stream.
func NewBigQueryManagedRecordWriter(
	ctx context.Context,
	serviceAccountJSON string,
	arrowSchema *arrow.Schema,
	project, dataset, table string,
	opts *BigQueryWriteOptions,
	clientOpts ...option.ClientOption,
) (*BigQueryManagedRecordWriter, error) {
	// Use default options if not provided.
	if opts == nil {
		opts = NewDefaultBigQueryWriteOptions()
	}
	if opts.Allocator == nil {
		opts.Allocator = memory.NewGoAllocator()
	}

	// Create the underlying BigQuery Storage Write client.
	bqWriteClient, err := NewBigQueryWriteClient(ctx, serviceAccountJSON, arrowSchema, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryWriteClient: %w", err)
	}

	// Create the managed writer client.
	mClient, err := managedwriter.NewClient(ctx, project, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create managed writer client: %w", err)
	}

	// Build the table resource name.
	parent := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table)

	// Retrieve table metadata to get the BigQuery schema.
	bqClient, err := bigquery.NewClient(ctx, project, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	defer bqClient.Close()

	tableRef := bqClient.Dataset(dataset).Table(table)
	md, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	// Convert the BigQuery schema to a Storage API table schema.
	storageSchema, err := BQSchemaToStorageTableSchema(md.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert BigQuery schema to Storage table schema: %w", err)
	}

	// Convert the storage schema to a DescriptorProto.
	// Note: the opts.UseWrapperTypes flag determines how STRING fields are emitted.
	schemaDescriptor, err := convertTableSchemaToDescriptor(storageSchema, opts.UseWrapperTypes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert storage schema to descriptor: %w", err)
	}

	// Create a pending write stream with the provided DataFormat and WriteStreamType.
	createReq := &storagepb.CreateWriteStreamRequest{
		Parent: parent,
		WriteStream: &storagepb.WriteStream{
			Type: opts.WriteStreamType,
		},
	}
	pendingStream, err := bqWriteClient.client.CreateWriteStream(ctx, createReq, bqWriteClient.callOptions.AppendRows...)
	if err != nil {
		return nil, fmt.Errorf("failed to create write stream: %w", err)
	}

	// Create a managed stream with the stream name and schema descriptor.
	managedStream, err := mClient.NewManagedStream(ctx,
		managedwriter.WithStreamName(pendingStream.GetName()),
		managedwriter.WithSchemaDescriptor(schemaDescriptor),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create managed stream: %w", err)
	}

	// Initialize the Arrow IPC buffer.
	buf := &bytes.Buffer{}

	return &BigQueryManagedRecordWriter{
		ctx:           ctx,
		writeClient:   bqWriteClient.client,
		managedClient: mClient,
		managedStream: managedStream,
		arrowSchema:   arrowSchema,
		offset:        0,
		mem:           opts.Allocator,
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

	// Reset the buffer and create a new IPC writer.
	w.buffer.Reset()
	ipcW := ipc.NewWriter(w.buffer,
		ipc.WithSchema(w.arrowSchema),
		ipc.WithAllocator(w.mem),
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
		return fmt.Errorf("failed to get append result: %w", err)
	}
	// Update offset (each record counts as one row for our simple example).
	w.offset = recvOffset + 1
	return nil
}

// Finalize finalizes the managed stream and commits the data to BigQuery.
// It returns the number of rows written.
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
		return 0, fmt.Errorf("stream errors during commit: %v", commitResp.GetStreamErrors())
	}
	return rowCount, nil
}

// Close gracefully closes the managed stream and releases associated resources.
func (w *BigQueryManagedRecordWriter) Close() error {
	// It is recommended to finalize (and commit) before closing.
	if err := w.managedStream.Close(); err != nil {
		return fmt.Errorf("failed to close managed stream: %w", err)
	}
	// Optionally close the managed writer client if no further writes will occur.
	if err := w.managedClient.Close(); err != nil {
		return fmt.Errorf("failed to close managed writer client: %w", err)
	}
	// Also close the underlying BigQuery Storage Write client.
	// (Assuming that you do not need to reuse it later.)
	if err := w.writeClient.Close(); err != nil {
		return fmt.Errorf("failed to close BigQuery Write client: %w", err)
	}
	return nil
}
