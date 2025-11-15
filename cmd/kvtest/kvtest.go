package main

import (
	"context"
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

	now := time.Now()
	st, err := newState(kv)
	if err != nil {
		logger.Error("failed to create state", slog.String("error", err.Error()))
		return
	}

	logger.Info("inited state", slog.Any("elapsed", time.Since(now)))

	now = time.Now()
	ctx := context.Background()
	err = createOrgs(ctx, st, 100)
	if err != nil {
		logger.Error("failed to create orgs", slog.String("error", err.Error()))
		return
	}

	/*
		maxCas := int64(0)
		st.patientCol.Select(ctx, func(fn sqlitekv.GetColumnValueFn) error {
			v, _, err := fn(0)
			if err != nil {
				return err
			}
			if v == nil {
				return nil
			}
			cas := v.(int64)
			if cas > maxCas {
				maxCas = cas
			}
			return nil
		}, sqlitekv.SelectOptions{
			Columns: []string{"cas"},
		})

		logger.Info("max cas", slog.Int64("maxCas", maxCas))
	*/

	logger.Info("kvtest completed", slog.Duration("duration", time.Since(now)))
}
