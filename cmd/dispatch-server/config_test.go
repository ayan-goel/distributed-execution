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
