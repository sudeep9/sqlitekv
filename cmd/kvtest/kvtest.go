package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sudeep9/sqlitekv"
)

type Org struct {
	RID  int    `json:rid"`
	Key  string `json:"key"`
	Name string `json:"name"`
}

type Patient struct {
	RID  int    `json:"rid"`
	Key  string `json:"key"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type Rx struct {
	RID        int    `json:"rid"`
	PatientId  string `json:"pid"`
	Medication string `json:"medication"`
}

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

	ctx := context.Background()
	org, err := kv.Collection("org", nil)
	if err != nil {
		logger.Error("failed to get org collection", slog.String("error", err.Error()))
		return
	}

	counter, err := sqlitekv.NewCounter(org, "/counter", sqlitekv.CounterOptions{})
	if err != nil {
		logger.Error("failed to create counter", slog.String("error", err.Error()))
		return
	}

	for i := 0; i < 20; i++ {
		nextCounter, err := counter.Next(ctx)
		if err != nil {
			logger.Error("failed to get next counter", slog.String("error", err.Error()))
			return
		}

		fmt.Printf("next counter: %d\n", nextCounter)
	}
}

func createOrg(ctx context.Context, col *sqlitekv.Collection, org *Org) (err error) {
	return
}
