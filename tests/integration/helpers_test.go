package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// kafkaTestEnv holds a running Kafka container and its broker address.
type kafkaTestEnv struct {
	container testcontainers.Container
	brokers   []string
}

func setupKafka(t *testing.T) *kafkaTestEnv {
	t.Helper()
	ctx := context.Background()

	kafkaContainer, err := tcKafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		tcKafka.WithClusterID("integration-test"),
	)
	if err != nil {
		t.Fatalf("failed to start kafka container: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get brokers: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	})

	return &kafkaTestEnv{
		container: kafkaContainer,
		brokers:   brokers,
	}
}

func createTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("failed to dial kafka: %v", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("failed to create topic %s: %v", topic, err)
	}
}

func produceMessages(t *testing.T, brokers []string, topic string, msgs []kafka.Message) {
	t.Helper()
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("failed to produce messages: %v", err)
	}
}

func consumeMessages(t *testing.T, brokers []string, topic, groupID string, count int, timeout time.Duration) []kafka.Message {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MaxWait:     1 * time.Second,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var msgs []kafka.Message
	for i := 0; i < count; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // timeout — return what we have
			}
			t.Fatalf("failed to read message: %v", err)
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func makeEventJSON(t *testing.T, id, eventType, userID string) []byte {
	t.Helper()
	event := map[string]interface{}{
		"id":        id,
		"type":      eventType,
		"user_id":   userID,
		"timestamp": time.Now().Format(time.RFC3339Nano),
		"payload": map[string]interface{}{
			"page":        "/home",
			"duration_ms": 1500,
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func makePastEventJSON(t *testing.T, id, eventType, userID string, timestamp time.Time) []byte {
	t.Helper()
	event := map[string]interface{}{
		"id":        id,
		"type":      eventType,
		"user_id":   userID,
		"timestamp": timestamp.Format(time.RFC3339Nano),
		"payload": map[string]interface{}{
			"page":        "/home",
			"duration_ms": 1500,
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newTestMetrics(namespace string) *testMetrics {
	return &testMetrics{namespace: namespace}
}

// testMetrics is a no-op metrics for integration tests to avoid
// prometheus duplicate registration panics across tests.
type testMetrics struct {
	namespace string
}

func formatTestName(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}
