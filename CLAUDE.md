# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`tpm-mongo-common` is a Go **library module** (not an application) in the GPA/TPM
ecosystem. It provides shared MongoDB building blocks: connection management, JSON-driven
operations, a change-stream consumption framework, distributed leases, and a mongo-backed
job/task scheduler. It targets the **official MongoDB Go driver v2**
(`go.mongodb.org/mongo-driver/v2`); `README.md` points to the v1→v2 migration guide because
this codebase was migrated.

Logging is `zerolog`. Every function defines a `const semLogContext = "pkg::func"` string
and attaches it to log lines — follow this idiom in new code.

## Commands

Standard Go tooling; there is no Makefile.

```sh
go build ./...
go vet ./...
go test ./...                              # see MongoDB requirement below
go test ./jsonops/...                      # one package
go test ./jobs/ -run TestWorker -v         # one test
```

### Tests need a real MongoDB

Most packages test against a **live local MongoDB at `mongodb://localhost:27017`**, not a
test container. The host, database name (e.g. `tpm_mongo_common`), and collection mappings
are hard-coded in each package's `main_test.go` / test `const` block. Tests fail (not skip)
when no server is reachable. Start a local mongo before running tests, or scope the run to
packages that don't need it (`util`, `changestream/keystring`, `changestream/events`).

### Regenerating entity code (`tpm-morphia-cli`)

The DAO-style code under `lease/`, `jobs/store/*`, and
`changestream/checkpoint/mdb/checkpointcollection/` is **generated** from YAML schemas by the
external `tpm-morphia-cli` tool, via the `gen-ents.sh` scripts (in `lease/morphia/`,
`jobs/store/_schemas/`, `changestream/checkpoint/mdb/`). Run that script from its own
directory to regenerate. Generated files carry `@tpm-schematics:start-region(...)` /
`end-region` markers: regeneration preserves code **inside** those regions and overwrites
everything else, so hand-written customizations go only inside a protected region.

## Architecture

### `mongolks` — connection registry (the foundation)

The central abstraction. A `LinkedService` wraps one configured mongo client/database.
`Initialize([]Config)` builds a **global registry** of named linked services;
`GetLinkedService(ctx, name)` / `GetCollection(ctx, instance, collectionId)` look them up and
lazily connect. Config is driver-agnostic YAML/mapstructure (pool, write/read concern, TLS,
compression, auth). A key indirection: callers reference collections by a logical
**`collectionId`**, which `Config.Collections` maps to the real collection **name** — so app
code never hard-codes collection names. Also here: `bulk-writer.go` (buffered bulk writes) and
`mongoprom/` (Prometheus instrumentation). Nearly every other package depends on `mongolks`.

### `jsonops` — operations described as JSON

Executes MongoDB operations whose filter/update/document/options are supplied as raw
`[]byte` JSON, keyed by `MongoJsonOperationStatementPart`. `NewOperation(opType, parts)`
builds an `Operation` (find, find-one, find-one-and-update, replace-one, update-one/-many,
delete-one/-many, insert-one, aggregate); `Operation.Execute(lks, collectionId)` runs it
against a `LinkedService` and returns an `OperationResult`. Lets callers store queries as data
rather than compiled Go.

### `changestream` — change-stream consumption framework (layered)

- **`events`** — parses a raw change-stream `bson.M` into a typed `ChangeEvent`
  (`ParseEvent` dispatches on `operationType`: insert/update/delete/replace). The lowest layer.
- **`keystring`** — decodes MongoDB's binary KeyString resume-token format (uses cgo,
  `import "C"`). Used to inspect/compare resume tokens.
- **`checkpoint`** — resume-token persistence behind the `ResumeTokenCheckpointSvc` interface
  (Retrieve / StoreIdle / CommitAt / Clear / OnHistoryLost). Implementations: `file/`
  (local file) and `mdb/` (a mongo collection); pick one via `checkpoint/factory`.
- **`watcherproducer`** — lower-level driver: opens a `$changeStream`, pushes each
  `ChangeEvent` to registered `WatcherListener`s (`ConsumeEvent` / `ConsumeBatch`).
- **`consumerproducer`** — higher-level server/consumer/processor model with batching and
  checkpoint-commit callbacks (`Server`, `ConsumerProducer`, `Processor` interfaces;
  `UnimplementedConsumerProducerProcessor` as an embeddable base). Commits resume tokens
  through the checkpoint service as batches succeed.

### `lease` — distributed lease / advisory lock

Coordinates singleton work across processes using a mongo collection as the lock store.
`AcquireLease(coll, groupId, objectId, auto)` returns a `Handler`; `RenewLease`/`Release`
manage its lifetime, with an optional background renew loop. Entity/filter/update code is
morphia-generated; `lease-handler.go` is the hand-written logic.

### `jobs` — mongo-backed job/task scheduler

A worker framework that runs jobs split into tasks. `jobs/worker/` holds the worker
implementation and driver loop; `jobs/duedatetrigger/` fires jobs on a due-date schedule;
`jobs/store/` holds the morphia-generated stores (`job`, `task`, `tasklog`, `ddtcheckpoint`).
Uses `lease` to ensure a single active worker and `mongolks` for storage.

### `util`

Low-level helpers: `MongoDbVersion` parsing, bson.D↔JSON decode helpers, error/log utilities.
Depended on by `mongolks` and others; depends on nothing else in this module.

## External dependency note

Beyond mongo-driver, this module depends on `tpm-common` (and the external `tpm-morphia-cli`
generator). These are separately-versioned sibling modules; bumping them means editing
`go.mod` here and running `go mod tidy` — the dependency sources themselves live in their own
repos and are released independently.
