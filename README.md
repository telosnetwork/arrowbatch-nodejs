# arrowbatch-nodejs

`yarn add @guilledk/arrowbatch-nodejs`

## Protocol

The ArrowBatch v1 protocol is a binary format that allows for streaming new rows to a file in a way that the files can be arbitrarily large while still retaining fast random access properties. It achieves this by sequentially appending random access Apache Arrow tables of a specific batch size plus a small header before each, to form a bigger table.

## File Structure

The ArrowBatch v1 file structure consists of a global header followed by a sequence of batches, each containing a batch header and the Arrow random access file bytes.

```
+-----------------+
|  Global Header  |
+-----------------+
|  Batch 0 Header |
+-----------------+
|    Arrow Table  |
|     Batch 0     |
+-----------------+
|  Batch 1 Header |
+-----------------+
|    Arrow Table  |
|     Batch 1     |
+-----------------+
|      ...        |
+-----------------+
```

### Global Header

The global header contains the following information:

- Version constant: ASCII string "ARROW-BATCH1"

### Batch Header

Each batch header contains the following fields:

- Batch header constant: ASCII string "ARROW-BATCH-TABLE"
- Batch byte size: 64-bit unsigned integer representing the size of the Arrow table batch in bytes
- Compression: 8-bit unsigned integer indicating the compression method used (0 for uncompressed, 1 for zstd)

## Streaming

The ArrowBatch v1 format can be streamed easily by reading each batch header as it comes in, then expecting to read the new full Arrow random access batch sent.

## Random Access

To perform random access on a disk ArrowBatch v1 file:

1. Read the global header.
2. Before reading any actual Arrow table data, read all Arrow batch headers in order by seeking around on the file using the specified metadata values on the batch headers.
3. Once the batch that contains the desired row is reached, read that batch and perform queries that only affect that small batch.

## Compression

The ArrowBatch v1 protocol supports compression of the Arrow table batches. The supported compression methods are:

- 0: Uncompressed
- 1: zstd (Zstandard compression)

The compression method is specified in the batch header.

## Data Model

The ArrowBatch v1 protocol defines a data model using Arrow table mappings. Each table mapping specifies the name, data type, and additional properties of the fields in the table.

The supported data types include:

- Unsigned integers (u8, u16, u32, u64, uintvar)
- Signed integers (i64)
- Bytes (string, bytes, base64)
- Digests (checksum160, checksum256)

The table mappings also allow specifying optional fields, fixed-length fields, arrays, and references to other tables.

## Conclusion

The ArrowBatch v1 protocol provides an efficient way to store and access large amounts of structured data while supporting streaming and random access. By leveraging the Apache Arrow format and incorporating compression, it offers flexibility and performance for various data processing scenarios.