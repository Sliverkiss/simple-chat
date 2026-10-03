package main

import (
	"strings"
	"testing"
)

func TestRedisHostLogDescriptionOmitsCredentialsAndPath(t *testing.T) {
	for _, input := range []string{
		"https://user:secret@example.test/private?token=secret",
		"rediss://user:secret@example.test:6380/private?token=secret",
	} {
		got := redisHost(input)
		if strings.Contains(got, "secret") || strings.Contains(got, "user") || strings.Contains(got, "private") || strings.Contains(got, "token=") {
			t.Fatalf("redis host log leaked URL component: %q", got)
		}
		if !strings.Contains(got, "example.test") {
			t.Fatalf("redis host log missing host: %q", got)
		}
	}
}
