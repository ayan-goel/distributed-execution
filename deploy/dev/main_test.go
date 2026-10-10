package main

import "testing"

func TestInitializationRejectsNonlocalStorageBeforeConnecting(t *testing.T) {
	for _, endpoint := range []string{"https://storage.example.org", "http://192.0.2.1:8333", "http://localhost:8333", "http://127.0.0.1:8333@storage.example.org", "http://127.0.0.1:8333/path"} {
		t.Run(endpoint, func(t *testing.T) {
			t.Setenv("DISPATCH_OBJECT_ENDPOINT", endpoint)
			t.Setenv("DISPATCH_S3_ACCESS_KEY", "development-access")
			t.Setenv("DISPATCH_S3_SECRET_KEY", "development-secret")
			if err := initialize(); err == nil {
				t.Fatal("accepted endpoint outside the local development profile")
			}
		})
	}
}
