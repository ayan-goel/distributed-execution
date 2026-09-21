package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/store"
)

func operator(ctx context.Context, args []string, out io.Writer) error {
	command := strings.Join(args[:min(2, len(args))], " ")
	var project, role, id string
	var cpu, memory int64
	var concurrency int
	f := flags(command)
	switch command {
	case "migrate":
		if len(args) != 1 {
			return errors.New("migrate takes no arguments")
		}
	case "project create":
		f.StringVar(&project, "name", "", "project name")
		f.Int64Var(&cpu, "cpu-millis", 4000, "CPU quota")
		f.Int64Var(&memory, "memory-mib", 8192, "memory quota")
		f.IntVar(&concurrency, "concurrency", 4, "active attempt quota")
	case "token create":
		f.StringVar(&project, "project", "", "project name")
		f.StringVar(&role, "role", "submit", "read, submit, or operator")
	case "token revoke":
		f.StringVar(&project, "project", "", "project name")
		f.StringVar(&id, "id", "", "token ID")
	default:
		return errors.New("unknown command")
	}
	if len(args) >= 2 {
		if err := f.Parse(args[2:]); err != nil {
			return err
		}
		if f.NArg() != 0 {
			return errors.New("unexpected arguments")
		}
	}
	if command != "migrate" && !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`).MatchString(project) {
		return errors.New("valid project name required")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	pool, err := openPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	switch command {
	case "migrate":
		return store.Migrate(ctx, pool)
	case "project create":
		if cpu <= 0 || cpu > 1_024_000 || memory <= 0 || memory > 16_777_216 || concurrency < 1 || concurrency > 1000 {
			return errors.New("project quota out of range")
		}
		if err := pool.QueryRow(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ($1,$2,$3,$4) RETURNING id::text", project, cpu, memory, concurrency).Scan(&id); err != nil {
			return errors.New("project creation failed (check name uniqueness and schema)")
		}
		return json.NewEncoder(out).Encode(map[string]string{"id": id, "name": project})
	case "token create":
		token, id, err := store.IssueToken(ctx, pool, project, store.Role(role))
		if err != nil {
			return err
		}
		// This is the sole intentional raw-token output. Operators must capture it
		// privately; serving code never includes tokens in logs or job metadata.
		return json.NewEncoder(out).Encode(map[string]string{"id": id, "token": token, "project": project, "role": role})
	case "token revoke":
		return store.RevokeToken(ctx, pool, project, id)
	}
	return nil
}
