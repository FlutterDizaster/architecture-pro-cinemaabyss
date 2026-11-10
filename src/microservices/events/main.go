package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var topics = map[string]string{
	"movie":   "movie-events",
	"user":    "user-events",
	"payment": "payment-events",
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// consume читает свой топик и пишет события в лог, это и есть «обработка» в MVP
func consume(brokers []string, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: "events-service",
		Topic:   topic,
	})
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("consumer %s: %v", topic, err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("consumed from %s [partition %d, offset %d]: %s", topic, m.Partition, m.Offset, m.Value)
	}
}

// produce пишет в партицию 0 через лидера и возвращает offset, который нужен в ответе по спецификации
func produce(ctx context.Context, broker, topic string, body []byte) (int32, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, 0)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, partition, offset, _, err := conn.WriteCompressedMessagesAt(nil, kafka.Message{Value: body})
	return partition, offset, err
}

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	for _, t := range topics {
		go consume(brokers, t)
	}

	http.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"status": true})
	})

	http.HandleFunc("/api/events/", func(w http.ResponseWriter, r *http.Request) {
		topic, ok := topics[strings.TrimPrefix(r.URL.Path, "/api/events/")]
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown event type"})
			return
		}
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		raw, err := io.ReadAll(r.Body)
		var buf bytes.Buffer
		if err != nil || json.Compact(&buf, raw) != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}
		body := buf.Bytes()

		partition, offset, err := produce(r.Context(), brokers[0], topic, body)
		if err != nil {
			log.Printf("produce to %s: %v", topic, err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		log.Printf("produced to %s [partition %d, offset %d]: %s", topic, partition, offset, body)
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":    "success",
			"partition": partition,
			"offset":    offset,
			"event":     json.RawMessage(body),
		})
	})

	port := env("PORT", "8082")
	log.Printf("events service listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
