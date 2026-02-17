package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type TopicPartition struct {
	Topic     string
	Partition int
}

type SourceKey struct {
	Cluster   string `json:"cluster"`
	Partition int    `json:"partition"`
	Topic     string `json:"topic"`
}

type OffsetValue struct {
	Offset int64 `json:"offset"`
}

type OffsetEntry struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Lag       int64  `json:"lag"`
}

var (
	mu          sync.Mutex
	offsets     = make(map[TopicPartition]int64)
	lastOffsets = make(map[TopicPartition]int64)

	sourceConns   = make(map[string]*kafka.Conn)
	sourceConnsMu sync.Mutex
)

func main() {
	sourceBroker := flag.String("source-broker", "localhost:9092", "Kafka bootstrap broker")
	destBroker := flag.String("dest-broker", "localhost:9093", "Kafka bootstrap broker")
	topic := flag.String("topic", "mm2-offsets.A.internal", "Topic to consume")
	group := flag.String("group", "mirrormaker-offset-checker", "Consumer group ID")
	cluster := flag.String("cluster", "A", "Source cluster name to filter on (must match the cluster in the offset messages)")
	stateFile := flag.String("state-file", "offsets.json", "Local state file path")
	refresh := flag.Duration("refresh", 10*time.Second, "Display refresh interval")
	flag.Parse()

	if err := fetchLastOffsets(*sourceBroker); err != nil {
		log.Fatalf("failed to fetch last offsets: %v", err)
	}

	if err := loadState(*stateFile); err != nil {
		log.Fatalf("failed to load state: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{*destBroker},
		Topic:       *topic,
		GroupID:     *group,
		StartOffset: kafka.FirstOffset,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go consumeLoop(ctx, reader, *cluster)
	go refreshLastOffsets(ctx, *sourceBroker, *refresh)

	ticker := time.NewTicker(*refresh)
	defer ticker.Stop()

	display()

	for {
		select {
		case <-ticker.C:
			display()
			if err := saveState(*stateFile); err != nil {
				log.Printf("failed to save state: %v", err)
			}
		case <-sigCh:
			cancel()
			fmt.Println("\nShutting down...")
			if err := saveState(*stateFile); err != nil {
				log.Printf("failed to save state on exit: %v", err)
			}
			if err := reader.Close(); err != nil {
				log.Printf("failed to close reader: %v", err)
			}
			closeSourceConns()
			return
		}
	}
}

func closeSourceConns() {
	sourceConnsMu.Lock()
	defer sourceConnsMu.Unlock()
	for key, conn := range sourceConns {
		conn.Close()
		delete(sourceConns, key)
	}
}

func getSourceConn(broker string) (*kafka.Conn, error) {
	sourceConnsMu.Lock()
	defer sourceConnsMu.Unlock()

	if conn, ok := sourceConns[broker]; ok {
		return conn, nil
	}

	log.Printf("Connecting to broker %s...", broker)
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return nil, err
	}
	sourceConns[broker] = conn
	return conn, nil
}

func getLeaderConn(addr, topic string, partition int) (*kafka.Conn, error) {

	sourceConnsMu.Lock()
	defer sourceConnsMu.Unlock()

	if conn, ok := sourceConns[addr]; ok {
		return conn, nil
	}

	log.Printf("Connecting to broker %s...", addr)
	conn, err := kafka.DialLeader(context.Background(), "tcp", addr, topic, partition)
	if err != nil {
		return nil, err
	}
	sourceConns[addr] = conn
	return conn, nil
}

func fetchLastOffsets(broker string) error {
	conn, err := getSourceConn(broker)
	if err != nil {
		return fmt.Errorf("cannot connect to broker: %w", err)
	}

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("cannot read partitions: %w", err)
	}

	result := make(map[TopicPartition]int64)
	for _, p := range partitions {
		leaderAddr := fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port)

		leaderConn, err := getLeaderConn(leaderAddr, p.Topic, p.ID)
		if err != nil {
			return fmt.Errorf("cannot connect to leader %s: %w", leaderAddr, err)
		}

		lastOffset, err := leaderConn.ReadLastOffset()
		if err != nil {
			return fmt.Errorf("cannot read last offset for %s/%d: %w", p.Topic, p.ID, err)
		}

		result[TopicPartition{Topic: p.Topic, Partition: p.ID}] = lastOffset
	}

	mu.Lock()
	for tp, offset := range result {
		lastOffsets[tp] = offset
	}
	mu.Unlock()

	return nil
}

func refreshLastOffsets(ctx context.Context, broker string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := fetchLastOffsets(broker); err != nil {
				log.Printf("failed to refresh last offsets: %v", err)
			}
		}
	}
}

func loadState(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var entries []OffsetEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	for _, e := range entries {
		offsets[TopicPartition{Topic: e.Topic, Partition: e.Partition}] = e.Offset
	}
	return nil
}

func saveState(path string) error {
	mu.Lock()
	entries := make([]OffsetEntry, 0, len(offsets))
	for tp, offset := range offsets {
		var lag int64
		if last, ok := lastOffsets[tp]; ok {
			lag = last - offset
			if lag < 0 {
				lag = 0
			}
		}
		entries = append(entries, OffsetEntry{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    offset,
			Lag:       lag,
		})
	}
	mu.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Topic != entries[j].Topic {
			return entries[i].Topic < entries[j].Topic
		}
		return entries[i].Partition < entries[j].Partition
	})

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func parseMessage(msg kafka.Message, cluster string) (TopicPartition, int64, bool) {
	if len(msg.Value) == 0 {
		return TopicPartition{}, 0, false
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(msg.Key, &raw); err != nil || len(raw) < 2 {
		return TopicPartition{}, 0, false
	}

	var sk SourceKey
	if err := json.Unmarshal(raw[1], &sk); err != nil {
		return TopicPartition{}, 0, false
	}

	if sk.Cluster != cluster {
		return TopicPartition{}, 0, false
	}

	var ov OffsetValue
	if err := json.Unmarshal(msg.Value, &ov); err != nil {
		return TopicPartition{}, 0, false
	}

	tp := TopicPartition{Topic: sk.Topic, Partition: sk.Partition}
	return tp, ov.Offset, true
}

func consumeLoop(ctx context.Context, reader *kafka.Reader, cluster string) {
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("read error: %v", err)
			continue
		}
		tp, offset, ok := parseMessage(msg, cluster)
		if !ok {
			continue
		}
		mu.Lock()
		offsets[tp] = offset
		mu.Unlock()
	}
}

func display() {
	mu.Lock()
	entries := make([]OffsetEntry, 0, len(offsets))
	for tp, offset := range offsets {
		var lag int64
		if last, ok := lastOffsets[tp]; ok {
			lag = last - offset
			if lag < 0 {
				lag = 0
			}
		}
		entries = append(entries, OffsetEntry{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    offset,
			Lag:       lag,
		})
	}
	mu.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Topic != entries[j].Topic {
			return entries[i].Topic < entries[j].Topic
		}
		return entries[i].Partition < entries[j].Partition
	})

	fmt.Print("\033[H\033[2J")
	fmt.Printf("MirrorMaker Offset Checker — %d topic-partitions — %s\n\n",
		len(entries), time.Now().Format("15:04:05"))

	if len(entries) == 0 {
		fmt.Println("No offset data yet.")
		return
	}

	topicWidth := len("TOPIC")
	for _, e := range entries {
		if len(e.Topic) > topicWidth {
			topicWidth = len(e.Topic)
		}
	}

	fmt.Printf("%-*s  %9s  %12s  %12s\n", topicWidth, "TOPIC", "PARTITION", "OFFSET", "LAG")
	fmt.Printf("%-*s  %9s  %12s  %12s\n", topicWidth, "-----", "---------", "------", "---")
	for _, e := range entries {
		fmt.Printf("%-*s  %9d  %12d  %12d\n", topicWidth, e.Topic, e.Partition, e.Offset, e.Lag)
	}
}
