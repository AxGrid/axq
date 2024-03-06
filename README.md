# AXQ

    Tool designed for consistent data storage in B2 and database

## Simple initialization examples:

### Writer:</br>
    Used to write data to a database by creating a blob with an id (fid)
    in which N messages are also stored with their own unique id 
```go
func main() {
    db, err := gorm.Open(mysql.Open(""), &gorm.Config{})
    if err != nil {
        panic(err)
    }

    w, err := axq.Writer().
        WithName("writer_name").
        WithDB(db).
        // MaxBlobSize specify max messages in blob (default 10000)
        WithMaxBlobSize(10000).
        // ChunkSize specify how much messages in chunk (default 1000)
        WithChunkSize(1000). 
        // WithoutCompression specify if you not want to compress your
        WithoutCompression().
        Build()
    if err != nil {
        panic(err)
    }
	
    err = w.Push([]byte("some msg"))
    if err != nil {
        panic(err)
    }	
}
```

### Reader: </br>
    Used to read data from Database and B2 if credentials 
    provided. Reading batches of blobs from db then sorting 
    before send to outer. Ordered number of messages guaranteed.
    Note that message id that can be sent to outer should be > reader.lastId
```go
	db, err := gorm.Open(mysql.Open(""), &gorm.Config{})
	if err != nil {
		panic(err)
	}

    r, err := axq.Reader().
        WithName("reader_name").
        WithDB(db).
        // LoaderCount start N loader workers
        WithLoaderCount(2).
        // WaiterCount start N outers processing sorter message
        WithWaiterCount(2).
        // BufferSize specify size of buffer chan for message queue
        WithBufferSize(100_000).
        // Provide only if you need read b2 before start reading db
        WithB2Credentials(creds).
        Build()
    if err != nil {
        panic(err)
    }

	for {
		m := r.Pop()
		// Handle message
		m.Done()
	}
```

### Archiver
    Used to archive data from database and send it as compressed and
    encrypted blob to B2
```go
func main() {
	db, err := gorm.Open(mysql.Open(""), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// B2 Application Key and Key ID should be exported to env
	creds := backblaze.Credentials{
		ApplicationKey: os.Getenv("B2_APP_KEY"),
		KeyID:          os.Getenv("B2_KEY_ID"),
	}
	a, err := axq.Archiver().
		WithName("archiver_name").
		WithDB(db).
		WithB2Credentials(creds).
		// MaxCount specify max messages in blob
		WithMaxCount(5_000_000). // Default 5_000_000
		// MaxCount specify max size of blob
		WithMaxSize(1_000_000). // Default 1_000_000
		// ChunkSize specify how often blob size recalculation will be made
		WithChunkSize(2_000). // Default 2_000
		Build()
	defer a.Close()
}
```

### B2 Reader
```go
    db, err := gorm.Open(mysql.Open(""), &gorm.Config{})
    if err != nil {
        panic(err)
    }
    
    // B2 Application Key and Key ID should be exported to env
    creds := backblaze.Credentials{
        ApplicationKey: os.Getenv("B2_APP_KEY"),
        KeyID:          os.Getenv("B2_KEY_ID"),
    }
    r, err := axq.B2Reader().
        WithName("reader_name").
        // LoaderCount start N loader workers
        WithLoaderCount(2).
        // WaiterCount start N outers processing sorter message
        WithOuterCount(2).
        // BufferSize specify size of buffer chan for message queue
        WithBufferSize(100_000).
        // Provide only if you need read b2 before start reading db
        WithB2Credentials(creds).
        Build()
    if err != nil {
        panic(err)
    }
    
    for {
        m := r.Pop()
        // Handle message
        m.Done()
    }
```

