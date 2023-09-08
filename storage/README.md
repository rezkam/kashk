# Key-Value Storage Engine in Go

## Overview

This repository contains a basic key-value persistent storage engine written in Go. The engine allows you to append key-value pairs to storage, delete them, and read them back efficiently using an in-memory index.

## Features

- **Append Key-Value Pairs**: Efficiently appends key-value pairs to a storage file.
- **Read Values by Key**: Quickly reads values from the storage file using a key.
- **Delete Key-Value Pairs**: Efficiently deletes key-value pairs by marking them as tombstones in the storage file.
- **Thread Safety**: Safe concurrent read and write access.
- **Size-Rotated Data Files**: Automatically switches to a new data file when the current file exceeds a specified size limit.
- **In-memory Indexing**: Utilizes an in-memory index to speed up data retrieval and deletions.

## Getting Started

### Installation

1. Clone this repository or download the source code.
2. Navigate to the directory containing the `main.go` and `storage.go` files.

### Usage

Here's how to create a new storage engine, append a key-value pair, read a value back, and delete a key-value pair:

```go
// Initialize new storage engine with a filename and maximum file size in bytes.
engine, err := NewEngine("data.dat", 1 * MB)
if err != nil {
	log.Fatal("Failed to initialize engine:", err)
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
