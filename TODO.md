# TODO

- [x] Integrate Travis / Codecov

- [x] Trim unused functionality

- [x] Replace BadgerDB with SQLite for better reliability

- [x] 90% test coverage (or better)

- [ ] Baseline benchmarks 10k * (500b logs, 1kb logs, 10kb logs)

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

    - [x] Querying across multiple domains/segments

- [ ] Untested Sad Paths:

    - [x] Err creating segment during flush

    - [x] Err flushing logs

    - [x] Err writing log KVs

    - [x] Err opening DB

    - [x] Err unmarshalling key during iteration

    - [x] Err unable to backfill

    - [x] Err reading segment as badger DB

    - [x] Err reading segment metadata