# TODO

- [x] Integrate Travis / Codecov

- [x] Trim unused functionality

- [ ] Replace BadgerDB with SQLite for better reliability

- [ ] 95% test coverage (or better)

- [ ] Better usage examples

- [ ] Thorough conceptual/behavioral (non-Godoc) docs

- [ ] Hosted docs

- [ ] Segment Deletion

    - [ ] `MarkSegmentForDeletion(path string) error`

    - [ ] Do not write to segments marked for deletion

    - [ ] Cannot mark active segment for deletion

    - [ ] `CleanupSegments() error`

- [x] Replace fmt.Printf/Println with structured/leveled logging (`Debug`, `Info`, `Warn`, `Error`)

- [ ] Untested Happy Paths:

    - [x] Should create new segment based on size

    - [x] Should create new segment based on duration

    - [x] Sort segments during scan

    - [x] More than 1 segment per domain during scan

    - [x] Sorting query results during delivery

    - [x] Segment creation during flush

    - [ ] Querying across multiple domains/segments

- [ ] Untested Sad Paths:

    - [x] Err creating segment during flush

    - [x] Err flushing logs

    - [ ] Err writing log KVs

        - This one is tricky, I'm having trouble forcing BadgerDB to return an error from a write. Open issue: https://github.com/dgraph-io/badger/issues/1159

    - [x] Err opening DB

    - [x] Err unmarshalling key during iteration

    - [x] Err unable to backfill

    - [x] Err reading segment as badger DB

    - [x] Err reading segment metadata