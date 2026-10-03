package main

import "testing"

func TestDefaultListenAddressIsLoopback(t *testing.T) {
	if got := defaultListenAddress(); got != "127.0.0.1:8080" {
		t.Fatalf("default listen address = %q, want loopback", got)
	}
}
