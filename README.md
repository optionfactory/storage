# OptionFactory Storage

A lightweight Java library for managing file storage across various backends, including the local filesystem, Amazon S3, and Google Cloud Storage (GCS).

## Features

- Unified `Storage` interface for all implementations.
- Support for **Amazon S3**.
- Support for **Google Cloud Storage**.
- **Local Filesystem** implementation.
- Permission management (Public Read / Private).
- `Zipper` utility for easy ZIP file compression and decompression.

## Installation

Add the desired dependency to your `pom.xml` file:

### Core (Interface and Utilities)
```xml
<dependency>
    <groupId>net.optionfactory</groupId>
    <artifactId>storage-core</artifactId>
    <version>4.0-SNAPSHOT</version>
</dependency>
```

### Amazon S3
```xml
<dependency>
    <groupId>net.optionfactory</groupId>
    <artifactId>storage-aws</artifactId>
    <version>4.0-SNAPSHOT</version>
</dependency>
```

### Google Cloud Storage
```xml
<dependency>
    <groupId>net.optionfactory</groupId>
    <artifactId>storage-gcp</artifactId>
    <version>4.0-SNAPSHOT</version>
</dependency>
```

## Usage

All implementations expose the `net.optionfactory.storage.Storage` interface.

### Filesystem Storage

Uses the local filesystem as the storage backend.

```java
Path basePath = Paths.get("/path/to/storage");
Storage storage = new FilesystemStorage(basePath);

// Store a file
storage.store("document.pdf", Paths.get("/local/path/doc.pdf"), Permissions.PRIVATE);

// Retrieve a file
InputStream is = storage.retrieve(Paths.get("document.pdf"));
```

### Amazon S3 Storage

Requires AWS credentials and the bucket name.

```java
Storage storage = new S3Storage(
    "accessKey", 
    "secretKey", 
    Region.EU_WEST_1, 
    "my-bucket", 
    3600 // cacheMaxAge in seconds
);

// Store with public read permissions
storage.store("image.jpg", Paths.get("photo.jpg"), Permissions.PUBLIC_READ);

// Get absolute URL
String url = storage.absoluteUrl("image.jpg");
```

### Google Cloud Storage

Requires GCP Project ID and Bucket ID.

```java
Storage storage = new GcpStorage(
    "my-project-id", 
    "my-bucket-id", 
    false // uniformBucketLevelAccess
);

storage.store(Path.of("report.csv"), Paths.get("data.csv"), Permissions.PRIVATE);
```

### Zipper Utility

The `Zipper` class provides static methods for simple ZIP archive management.

```java
// Decompression
List<Path> files = Zipper.decompress(Paths.get("/target/dir"), Paths.get("archive.zip"));

// File compression
Path zipPath = Zipper.compress(List.of(Paths.get("file1.txt"), Paths.get("file2.txt")), Paths.get("output.zip"));

// In-memory compression
Map<String, InputStream> streams = Map.of("data.txt", new ByteArrayInputStream("content".getBytes()));
byte[] zipData = Zipper.compress(streams);
```

## Requirements

- Java 25
- Maven 3.6.3+

## License

This library is distributed under the BSD 3-Clause License. See the `LICENSE` file for details.
