# GraniteDB storage format

GraniteDB stores data in fixed-size 4 KB pages. Each database file begins with a metadata header page followed by user pages. All multi-byte integers are encoded using little-endian order.

## Page 0 – database header

The header page records global metadata and the serialised catalogue.

```
+---------------------+----------------------------------------------------+
| Offset              | Description                                        |
+=====================+====================================================+
| 0x00 (8 bytes)      | Magic number "GRANITED"                             |
| 0x08 (2 bytes)      | Format version (current: 1)                         |
| 0x0C (4 bytes)      | Total page count                                    |
| 0x10 (4 bytes)      | Free list head page id (0xFFFFFFFF = none)         |
| 0x14 (4 bytes)      | Size of catalogue payload in bytes                 |
| 0x18..0xFFF         | Serialised catalogue payload                       |
+---------------------+----------------------------------------------------+
```

The catalogue payload captures table and column metadata. It is stored entirely within the header page to simplify bootstrap.

## Heap page layout

Every table uses a heap file – a linked list of slotted pages that hold row data.

```
+-----------------------+--------------------------------------------------+
| Offset                | Description                                      |
+=======================+==================================================+
| 0x00 (4 bytes)        | Next page id (0 means end of list)               |
| 0x04 (2 bytes)        | Slot count                                       |
| 0x06 (2 bytes)        | Start of free space                              |
| 0x08 (2 bytes)        | Start of slot directory (grows backwards)        |
| 0x0A..0x0F            | Reserved                                         |
| 0x10..                | Record data region (grows upwards)               |
| ...                   | Free space                                       |
| 0x1000 - 4*slots..    | Slot directory entries (offset, length per slot) |
+-----------------------+--------------------------------------------------+
```

Each slot directory entry stores a 16-bit offset and 16-bit length pointing into the record data region. Records are never moved once written; new rows append to the data region until space runs out, at which point a new page is linked to the heap file.

## Record layout

Records are encoded sequentially according to the table schema. The encoding relies on column order and does not include field identifiers. The supported column types map to bytes as follows:

```
+----------------+-------------------------+
| Column type    | Encoding                 |
+================+=========================+
| INT            | 4-byte signed integer    |
| BIGINT         | 8-byte signed integer    |
| BOOLEAN        | 1 byte (0 = false, 1 = true) |
| VARCHAR(n)     | 2-byte length + UTF-8 bytes |
| DATE           | 4-byte day count since 1970-01-01 |
| TIMESTAMP      | 8-byte Unix nanoseconds (UTC) |
+----------------+-------------------------+
```

VARCHAR values are limited to 65,535 bytes due to the 16-bit length prefix. DATE values are normalised to midnight UTC before encoding. TIMESTAMP values are stored as UTC instants with nanosecond precision.

This layout keeps the data structures small and simple while providing enough flexibility for variable-length columns. Future releases will build on this foundation to add indexes, logging, and richer query capabilities.
