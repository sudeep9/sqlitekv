package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/sudeep9/sqlitekv"
)

type Person struct {
	BaseCollection
	FirstName string `json:"fname"`
	LastName  string `json:"lname"`
	Phone     string `json:"-"`
	Age       int    `json:"age"`
}

func (p *Person) Column(i int, name string) (ok bool, val any, err error) {
	switch name {
	case "phone":
		return true, p.Phone, nil
	default:
		return false, nil, nil
	}
}

func (p *Person) SetColumn(i int, name string, ok bool, val any) error {
	switch name {
	case "phone":
		if ok {
			p.Phone = val.(string)
		}
		return nil
	}
	return nil
}

func main() {
	ctx := context.Background()
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
	err = createOrgs(ctx, st, 100)
	if err != nil {
		logger.Error("failed to create orgs", slog.String("error", err.Error()))
		return
	}

	logger.Info("kvtest completed", slog.Duration("duration", time.Since(now)))
}
