# Key-Value Storage Engine in Go

## Overview

Kashk Storage package contains a versatile and configurable key-value storage engine written in Go. The engine offers efficient key-value storage, retrieval, and deletion operations, optimized for concurrency and featuring automatic size based data file rotation.

## Features

- **Put Key-Value Pairs**: Efficiently put key-value pairs into to the storage file almost similar to performance of writing to a file.
- **In-Memory Indexing**: Utilizes an in-memory index for quick data retrieval.
- **Thread-Safe**: It provides safe concurrent read and write access via a read-write mutex.
- **Read Values by Key**: Retrieve values quickly using an in-memory index.
- **Delete Key-Value Pairs**: Efficiently delete key-value pairs by marking them with a tombstone value.
- **Customizable File Size**: Set the maximum size for each log file. A new file will be used when the current one exceeds this limit.
- **Customizable Key Size**: Control the maximum allowed size for keys.
- **Customizable File Names**: You can set the name for the data file.
- **Customizable Tombstone Value**: You can define the tombstone value used for marking deleted entries.


## Getting Started

### Installation

1. Clone this repository or download the source code.
2. Navigate to the directory containing the `storage.go` file.

### Usage

Here's how to create a new storage engine and use its features:

```go
import "path-to-storage-package/storage"

// Initialize a new storage engine with default settings
engine, err := storage.NewEngine()
if err != nil {
    log.Fatal("Failed to initialize engine:", err)
}

// Initialize with options
engine, err := storage.NewEngine(
    storage.WithMaxLogSize(10 * storage.MB),
    storage.WithMaxKeySize(1 * storage.KB),
    storage.WithFileName("custom_data.dat"),
    storage.WithTombStone("custom_tombstone")
)
if err != nil {
    log.Fatal("Failed to initialize engine with options:", err)
}

// Put a key-value pair
err = engine.Put("name", "John Doe")
if err != nil {
    log.Fatal("Failed to append key-value pair:", err)
}

// Read a value back
value, err := engine.Get("name")
if err != nil {
    log.Fatal("Failed to get value:", err)
}

// Delete a key-value pair
err = engine.Delete("name")
if err != nil {
    log.Fatal("Failed to delete key-value pair:", err)
}
