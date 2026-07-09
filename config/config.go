package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	RedisAddr        string
	RedisPassword    string
	RedisDB          int
	ServerPort       string
	WorkerCount      int
	Queues           []string
	ScheduleInterval time.Duration
	MaxPriority      int
	MaxRetries       int
	WebHookSecret    string
}

func Load() Config {
	godotenv.Load()
	return Config{
		RedisAddr:        getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    getEnv("REDIS_PASSWORD", ""),
		RedisDB:          getEnvInt("REDIS_DB", 0),
		ServerPort:       getEnv("SERVER_PORT", ":8080"),
		WorkerCount:      getEnvInt("WORKER_COUNT", 10),
		Queues:           strings.Split(getEnv("QUEUES", "email,mobile"), ","),
		ScheduleInterval: time.Duration(getEnvInt("SCHEDULE_INTERVAL_SEC", 5)) * time.Second,
		MaxPriority:      getEnvInt("MAX_PRIORITY", 10),
		MaxRetries:       getEnvInt("MAX_RETRIES", 10),
		WebHookSecret:    getEnv("WEBHOOK_SECRET", ""),
	}
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallBack int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallBack
}
