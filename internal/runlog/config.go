// Package runlog implements event ingestion and storage for run histories.
package runlog

import "os"

// Config contains service addresses and storage settings.
type Config struct {
	HTTP     string
	PG       string
	Mongo    string
	Database string
	Brokers  string
	Topic    string
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// LoadConfig reads configuration from the environment with local defaults.
func LoadConfig() Config {
	return Config{
		HTTP:     env("HTTP_ADDR", ":8080"),
		PG:       env("POSTGRES_URL", "postgres://runlog:runlog@localhost:5432/runlog?sslmode=disable"),
		Mongo:    env("MONGO_URL", "mongodb://localhost:27017"),
		Database: env("MONGO_DATABASE", "runlog"),
		Brokers:  env("KAFKA_BROKERS", "localhost:9092"),
		Topic:    env("KAFKA_TOPIC", "runlog.events"),
	}
}
