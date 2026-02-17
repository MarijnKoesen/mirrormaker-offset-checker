# MirrorMaker Offset Checker

A CLI tool that consumes a Kafka topic containing MirrorMaker 2 offset sync messages and displays a live table of all topic-partitions and their current offsets.

## How it works

MirrorMaker 2 stores offset translation data in an internal topic on the destination.

So say you are mirroring from cluster `A` to cluster `B`, then it will create a topic on cluster `B` called `mm2-offsets.A.internal`. 

Each message has:

- **Key**: a JSON array `["MirrorSourceConnector", {"cluster": "A", "partition": 0, "topic": "my-topic"}]`
- **Value**: a JSON object `{"offset": 12345}`

This tool connects a consumer group to your cluster that reads that compacted topic from the beginning, parses every message, and maintains an in-memory map of topic-partition to offset. A ticker refreshes the terminal display every second (configurable) and persists the current state to a local JSON file. On restart, the state file is loaded so previously seen offsets are displayed immediately while the consumer catches up.

Graceful shutdown on SIGINT/SIGTERM saves state before exiting.

## Usage

```
go run . [flags]
```

Or build and run:

```
go build -o mirrormaker-offset-checker .
./mirrormaker-offset-checker [flags]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `-broker` | `localhost:9092` | Kafka bootstrap broker |
| `-topic` | `mm2-offsets.A.internal` | Topic to consume |
| `-cluster` | `A` | Source cluster name to filter on (must match the cluster in the offset messages) |
| `-group` | `mirrormaker-offset-checker` | Consumer group ID |
| `-state-file` | `offsets.json` | Local state file path |
| `-refresh` | `30s` | Display refresh interval |

**Important:** The `-cluster` flag must match the source cluster alias used by MirrorMaker 2. The offset topic contains entries with a `cluster` field in each message key — only messages matching this value are processed. If you're mirroring from cluster `A` to cluster `B`, the cluster value is `A`.

### Example

```
go run . -broker kafka.example.com:9092 -topic mm2-offsets.A.internal -cluster A
```

Output:

```
MirrorMaker Offset Checker — 3 topic-partitions — 14:32:05

TOPIC        PARTITION  OFFSET
-----        ---------  ------
orders               0  48201
orders               1  51037
users                0  12840
```

## Requirements

- Go 1.23+
- Network access to a Kafka broker with MirrorMaker 2 offset data
