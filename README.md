# Kashk Storage

## Overview

Kashk Storage is a high-performance, persistent key-value storage engine. Kashk offers efficient key-value storage, retrieval, and deletion 
operations optimized for concurrency, featuring automatic size-based data file rotation. 

## Features

- Efficiently put key-value pairs into the storage engine. Kashk is super fast, and its insertion performance is almost similar to the 
performance of appending to a file.
- Kashk utilizes an in-memory index for quick data retrieval.
- It provides safe concurrent read and write access and uses the in-memory index for quick search. Reading the values only needs one seek 
operation.
- Efficiently delete key-value pairs by marking them with a tombstone value.

## Getting Started

### Installation

1. Clone this repository or download the source code.
2. Create a data path and give the user permission to read and write.

### Usage

Here's how to create a new storage engine and use its features:

```go
import "path-to-storage-package/storage"

// Initialize a new storage engine with default settings
engine, err := storage.NewEngine("./data-path/")
if err != nil {
    log.Fatal("Failed to initialize engine:", err)
}

// Initialize with options
engine, err := storage.NewEngine("./data-path/",
    storage.WithMaxLogSize(10 * storage.MB),
    storage.WithMaxKeySize(1 * storage.KB),
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

