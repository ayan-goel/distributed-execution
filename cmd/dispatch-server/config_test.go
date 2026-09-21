package main

import "testing"

func TestServingConfigurationRequiresSecureListener(t *testing.T) {
	for _, tc := range []struct {
		args  []string
		valid bool
	}{
		{[]string{"--dev-insecure", "--listen", "127.0.0.1:0", "--allow-registry", "index.docker.io"}, true},
		{[]string{"--dev-insecure", "--listen", "0.0.0.0:8080", "--allow-registry", "index.docker.io"}, false},
		{[]string{"--listen", ":8080", "--allow-registry", "index.docker.io"}, false},
		{[]string{"--listen", ":8443", "--tls-cert", "cert.pem", "--tls-key", "key.pem", "--allow-registry", "index.docker.io"}, true},
		{[]string{"--dev-insecure", "--allow-registry", "https://bad.example"}, false},
		{[]string{"--dev-insecure"}, false},
	} {
		_, err := parseServeConfig(tc.args)
		if (err == nil) != tc.valid {
			t.Fatalf("args %v: %v", tc.args, err)
		}
	}
}

func TestWorkerListenerAlwaysRequiresCompleteMTLSConfiguration(t *testing.T) {
	base := []string{"--dev-insecure", "--listen", "127.0.0.1:0", "--allow-registry", "index.docker.io"}
	for _, tc := range []struct {
		args  []string
		valid bool
	}{
		{[]string{"--worker-listen", "127.0.0.1:0", "--worker-tls-cert", "server.pem", "--worker-tls-key", "server.key", "--worker-client-ca", "ca.pem"}, true},
		{[]string{"--worker-listen", ":8444"}, false},
		{[]string{"--worker-listen", ":8444", "--worker-tls-cert", "server.pem", "--worker-tls-key", "server.key"}, false},
		{[]string{"--worker-client-ca", "ca.pem"}, false},
		{[]string{"--worker-listen", "not-an-address", "--worker-tls-cert", "server.pem", "--worker-tls-key", "server.key", "--worker-client-ca", "ca.pem"}, false},
	} {
		_, err := parseServeConfig(append(append([]string{}, base...), tc.args...))
		if (err == nil) != tc.valid {
			t.Fatalf("worker listener config mismatch: %v: %v", tc.args, err)
		}
	}
}
