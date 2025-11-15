package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/sudeep9/sqlitekv"
)

func main() {
	logger := slog.Default()
	logger.Info("kvtest started")

	kv, err := sqlitekv.Open("./tmp/test.db", &sqlitekv.Options{
		JournalMode: "WAL",
	})
	if err != nil {
		logger.Error("failed to open kv store", slog.String("error", err.Error()))
		return
	}
	defer kv.Close()

	st, err := newState(kv)
	if err != nil {
		logger.Error("failed to create state", slog.String("error", err.Error()))
		return
	}

	now := time.Now()
	ctx := context.Background()
	err = createOrgs(ctx, st, 100)
	if err != nil {
		logger.Error("failed to create orgs", slog.String("error", err.Error()))
		return
	}

	logger.Info("data generation completed", slog.Duration("duration", time.Since(now)))

	listOpts := sqlitekv.ListOptions{
		OrderBy: []string{
			"name desc",
		},
	}

	err = st.patientCol.List(ctx, "/o/62/p/", func(rid int64, key string, buf []byte) error {
		var patient Patient
		err := json.Unmarshal(buf, &patient)
		if err != nil {
			return err
		}
		logger.Info("patient",
			slog.Int64("rid", rid),
			slog.String("key", key),
			slog.Int64("cas", patient.Meta.Cas),
			slog.String("name", patient.Name),
			slog.Int("age", patient.Age))
		return nil
	}, listOpts)

	if err != nil {
		logger.Error("failed to list patients", slog.String("error", err.Error()))
		return
	}
}
