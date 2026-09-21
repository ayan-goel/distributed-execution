//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestProjectTokenPermissionsAndRevocation(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4),('other',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	for _, role := range []Role{RoleRead, RoleSubmit, RoleOperator} {
		token, id, err := IssueToken(ctx, pool, "research", role)
		if err != nil {
			t.Fatal(err)
		}
		if len(token) != 47 || !strings.HasPrefix(token, "dsp_") {
			t.Fatal("unexpected token format")
		}
		p, err := Authenticate(ctx, pool, token)
		if err != nil || p.Project != "research" || p.TokenID != id || p.Role != role {
			t.Fatal("wrong token identity", err)
		}
		if !p.Allows(RoleRead) || p.Allows(RoleOperator) != (role == RoleOperator) || p.Allows(RoleSubmit) != (role != RoleRead) || p.Allows(Role("unknown")) {
			t.Fatal("incorrect permissions")
		}
		var hash []byte
		if err := pool.QueryRow(ctx, "SELECT token_hash FROM api_tokens WHERE id=$1", id).Scan(&hash); err != nil || len(hash) != 32 || string(hash) == token {
			t.Fatal("token storage must contain only a hash", err)
		}
		if err := RevokeToken(ctx, pool, "other", id); !errors.Is(err, ErrNotFound) {
			t.Fatal("cross-project revocation accepted", err)
		}
		if err := RevokeToken(ctx, pool, "research", id); err != nil {
			t.Fatal(err)
		}
		if err := RevokeToken(ctx, pool, "research", id); err != nil {
			t.Fatal("repeated revocation failed", err)
		}
		if _, err := Authenticate(ctx, pool, token); !errors.Is(err, ErrUnauthorized) {
			t.Fatal("revoked token accepted", err)
		}
	}
	for _, token := range []string{"", "wrong", "dsp_" + strings.Repeat("a", 43)} {
		if _, err := Authenticate(ctx, pool, token); !errors.Is(err, ErrUnauthorized) {
			t.Fatal("invalid token accepted", err)
		}
	}
	if _, _, err := IssueToken(ctx, pool, "research", Role("admin")); !errors.Is(err, ErrInvalid) {
		t.Fatal("unsupported role accepted", err)
	}
	var events int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM audit_events").Scan(&events); err != nil || events != 6 {
		t.Fatal("expected one issuance and revocation event per token", events, err)
	}
}

func TestDisabledProjectRejectsTokens(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	token, _, err := IssueToken(ctx, pool, "research", RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE projects SET enabled=false WHERE name='research'"); err != nil {
		t.Fatal(err)
	}
	if _, err := Authenticate(ctx, pool, token); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("disabled project authenticated", err)
	}
	if _, _, err := IssueToken(ctx, pool, "research", RoleRead); !errors.Is(err, ErrDisabled) {
		t.Fatal("issued token for disabled project", err)
	}
}
