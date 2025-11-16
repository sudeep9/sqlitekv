package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sudeep9/sqlitekv"
)

func main() {
	ctx := context.Background()
	logger := slog.Default()
	logger.Info("kvtest started")

	wg := sync.WaitGroup{}

	now := time.Now()
	count := 10
	totalOrgs := 50
	orgCount := totalOrgs / count

	for i := range count {
		wg.Go(func() {
			dbpath := fmt.Sprintf("./tmp/test%d.db", i)
			err := generateData(ctx, logger, dbpath, orgCount)
			if err != nil {
				logger.Error("failed to generate data", slog.String("error", err.Error()))
				return
			}
		})
	}

	wg.Wait()

	logger.Info("kvtest completed", slog.Duration("duration", time.Since(now)))
}

func generateData(ctx context.Context, logger *slog.Logger, dbpath string, orgCount int) (err error) {
	kv, err := sqlitekv.Open(dbpath, &sqlitekv.Options{
		JournalMode: "WAL",
	})
	if err != nil {
		logger.Error("failed to open kv store", slog.String("error", err.Error()))
		return
	}
	defer kv.Close()

	st, err := newState(logger, kv)
	if err != nil {
		logger.Error("failed to create state", slog.String("error", err.Error()))
		return
	}

	err = createOrgs(ctx, st, orgCount)
	return err

}
