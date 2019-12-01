# TODO

- [x] Integrate Travis / Codecov

- [x] Trim unused functionality

- [ ] Better usage examples

- [ ] Thorough hosted docs

- [ ] Segment Deletion

    - [ ] `MarkSegmentForDeletion(path string) error`

    - [ ] Do not write to segments marked for deletion

    - [ ] Cannot mark active segment for deletion

    - [ ] `CleanupSegments() error`

- [ ] Replace fmt.Printf/Println with structured/leveled logging (`Debug`, `Info`, `Warn`, `Error`)

- [ ] Untested Happy Paths:

    - [ ] Create new segment based on size

    - [ ] Create new segment based on duration

    - [ ] Sort segments during scan

    - [ ] More than 1 segment per domain during scan

- [ ] Untested Sad Paths:

    - [ ] Err creating segment during flush

    - [ ] Err flushing logs

    - [ ] Err writing log KVs

    - [ ] Err opening DB

    - [ ] Err unmarshalling key during iteration

    - [ ] Err matching log to domain after enqueued

    - [ ] Err unable to backfill

    - [ ] Err reading segment as badger DB

    - [ ] Err reading segment metadata