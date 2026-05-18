package testutil

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupRedis(t *testing.T) *redis.Client {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	t.Cleanup(func() {
		err := c.Terminate(ctx)
		if err != nil {
			t.Errorf("Unable to terminate container")
		}
	})

	ip, err := c.Host(ctx)
	if err != nil {
		t.Fatal("unable to get container ip")
	}

	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatal("unable to fetch container mapped port")
	}

	return redis.NewClient(&redis.Options{
		Addr: ip + ":" + port.Port(),
		DB:   0,
	})
}
