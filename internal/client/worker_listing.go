package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"
)

type WorkerListOptions struct {
	Limit  int
	Cursor string
}

type WorkerCapacity struct {
	CPUMillis  int64 `json:"cpuMillis"`
	MemoryMiB  int64 `json:"memoryMiB"`
	ScratchMiB int64 `json:"scratchMiB"`
	Slots      int64 `json:"slots"`
}

type WorkerSummary struct {
	ID                     string            `json:"id"`
	Name                   string            `json:"name"`
	State                  string            `json:"state"`
	Labels                 map[string]string `json:"labels"`
	DrainRequested         bool              `json:"drainRequested"`
	RuntimeHealthy         bool              `json:"runtimeHealthy"`
	ReconciliationComplete bool              `json:"reconciliationComplete"`
	DiskPressure           bool              `json:"diskPressure"`
	LastHeartbeatAt        *time.Time        `json:"lastHeartbeatAt"`
	Capacity               WorkerCapacity    `json:"capacity"`
	Reserved               WorkerCapacity    `json:"reserved"`
	Available              WorkerCapacity    `json:"available"`
}

type WorkerPage struct {
	Project    string          `json:"project"`
	ProjectID  string          `json:"projectId"`
	AsOf       time.Time       `json:"asOf"`
	Workers    []WorkerSummary `json:"workers"`
	HasMore    bool            `json:"hasMore"`
	NextCursor string          `json:"nextCursor"`
}

func (c *Client) ListWorkers(ctx context.Context, options WorkerListOptions) (WorkerPage, error) {
	if options.Limit < 1 || options.Limit > 100 || options.Cursor != "" && !visibleASCII(options.Cursor, 256) {
		return WorkerPage{}, errors.New("invalid worker listing options")
	}
	q := url.Values{"limit": {strconv.Itoa(options.Limit)}}
	if options.Cursor != "" {
		q.Set("cursor", options.Cursor)
	}
	body, err := c.requestBodyLimit(ctx, http.MethodGet, "/v1/workers?"+q.Encode(), nil, "", 1<<20)
	if err != nil {
		return WorkerPage{}, err
	}
	return decodeWorkerPage(body, options)
}

func decodeWorkerCapacity(raw []byte) (WorkerCapacity, error) {
	var capacity WorkerCapacity
	err := requiredDiagnosticObject(raw, map[string]any{"cpuMillis": &capacity.CPUMillis, "memoryMiB": &capacity.MemoryMiB, "scratchMiB": &capacity.ScratchMiB, "slots": &capacity.Slots})
	if err != nil || capacity.CPUMillis < 0 || capacity.MemoryMiB < 0 || capacity.ScratchMiB < 0 || capacity.Slots < 0 {
		return WorkerCapacity{}, errors.New("invalid worker capacity")
	}
	return capacity, nil
}

func decodeWorkerPage(body []byte, options WorkerListOptions) (WorkerPage, error) {
	invalid := errors.New("invalid worker listing response")
	var page WorkerPage
	var rows []json.RawMessage
	if !utf8.Valid(body) || requiredDiagnosticObject(body, map[string]any{"project": &page.Project, "projectId": &page.ProjectID, "asOf": &page.AsOf, "workers": &rows, "hasMore": &page.HasMore, "nextCursor": &page.NextCursor}) != nil || !artifactName.MatchString(page.Project) || !downloadUUID(page.ProjectID) || !validDiagnosticTime(page.AsOf.UTC()) || rows == nil || len(rows) > options.Limit {
		return WorkerPage{}, invalid
	}
	if page.HasMore && (len(rows) == 0 || !visibleASCII(page.NextCursor, 256) || page.NextCursor == options.Cursor) || !page.HasMore && page.NextCursor != "" {
		return WorkerPage{}, invalid
	}
	page.AsOf = page.AsOf.UTC()
	page.Workers = make([]WorkerSummary, len(rows))
	for i, raw := range rows {
		worker := &page.Workers[i]
		var labels, capacity, reserved, available json.RawMessage
		if requiredNullableDiagnosticObject(raw, map[string]any{
			"id": &worker.ID, "name": &worker.Name, "state": &worker.State, "labels": &labels,
			"drainRequested": &worker.DrainRequested, "runtimeHealthy": &worker.RuntimeHealthy,
			"reconciliationComplete": &worker.ReconciliationComplete, "diskPressure": &worker.DiskPressure,
			"lastHeartbeatAt": &worker.LastHeartbeatAt, "capacity": &capacity, "reserved": &reserved, "available": &available,
		}, "lastHeartbeatAt") != nil || !downloadUUID(worker.ID) || !artifactName.MatchString(worker.Name) || !validWorkerState(worker.State) || i > 0 && worker.ID <= page.Workers[i-1].ID {
			return WorkerPage{}, invalid
		}
		if worker.LastHeartbeatAt != nil {
			utc := worker.LastHeartbeatAt.UTC()
			if !validDiagnosticTime(utc) {
				return WorkerPage{}, invalid
			}
			worker.LastHeartbeatAt = &utc
		}
		if json.Unmarshal(labels, &worker.Labels) != nil || worker.Labels == nil || len(worker.Labels) > 64 || worker.Labels["os"] != "linux" || (worker.Labels["architecture"] != "amd64" && worker.Labels["architecture"] != "arm64") {
			return WorkerPage{}, invalid
		}
		fields := map[string]any{}
		for key, value := range worker.Labels {
			if !artifactName.MatchString(key) || len(value) == 0 || len(value) > 256 {
				return WorkerPage{}, invalid
			}
			for _, r := range value {
				if unicode.IsControl(r) {
					return WorkerPage{}, invalid
				}
			}
			fields[key] = new(string)
		}
		if requiredDiagnosticObject(labels, fields) != nil {
			return WorkerPage{}, invalid
		}
		var err error
		if worker.Capacity, err = decodeWorkerCapacity(capacity); err != nil {
			return WorkerPage{}, invalid
		}
		if worker.Reserved, err = decodeWorkerCapacity(reserved); err != nil {
			return WorkerPage{}, invalid
		}
		if worker.Available, err = decodeWorkerCapacity(available); err != nil {
			return WorkerPage{}, invalid
		}
		c, r := worker.Capacity, worker.Reserved
		if c.CPUMillis < 1 || c.CPUMillis > 1_024_000 || c.MemoryMiB < 1 || c.MemoryMiB > 16_777_216 || c.ScratchMiB < 1 || c.ScratchMiB > 1_073_741_824 || c.Slots < 1 || c.Slots > 1000 {
			return WorkerPage{}, invalid
		}
		// Reservations may exceed reduced claims. Reject inconsistent free values
		// without discarding that evidence or inferring current schedulability.
		if worker.Available != (WorkerCapacity{CPUMillis: max(0, c.CPUMillis-r.CPUMillis), MemoryMiB: max(0, c.MemoryMiB-r.MemoryMiB), ScratchMiB: max(0, c.ScratchMiB-r.ScratchMiB), Slots: max(0, c.Slots-r.Slots)}) {
			return WorkerPage{}, invalid
		}
	}
	return page, nil
}
