package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/sudeep9/sqlitekv"
)

type Org struct {
	RID  int64
	Key  string
	Name string `json:"name"`
}

type Patient struct {
	RID  int64
	Key  string
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type Rx struct {
	RID        int64
	Key        string
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
	orgCol, err := kv.Collection("org", &sqlitekv.CollectionOptions{
		Delimiter: "/",
	})
	if err != nil {
		logger.Error("failed to get org collection", slog.String("error", err.Error()))
		return
	}

	//counter, err := sqlitekv.NewCounter(orgCol, "/counter", sqlitekv.CounterOptions{})
	//if err != nil {
	//	logger.Error("failed to create counter", slog.String("error", err.Error()))
	//	return
	//}

	err = orgCol.List(ctx, "/o/123/p/", func(rid int64, key string, buf []byte) error {
		var patient Patient
		err := json.Unmarshal(buf, &patient)
		if err != nil {
			return err
		}
		logger.Info("patient", slog.Int64("rid", rid), slog.String("key", key), slog.String("name", patient.Name), slog.Int("age", patient.Age))
		return nil
	})
	if err != nil {
		logger.Error("failed to list patients", slog.String("error", err.Error()))
		return
	}
}

func createOrg(ctx context.Context, col *sqlitekv.Collection, counter *sqlitekv.Counter, org *Org) (err error) {
	buf, err := json.Marshal(org)
	if err != nil {
		return
	}

	org.RID, err = col.Put(ctx, org.Key, buf)
	if err != nil {
		return
	}

	err = createPatients(ctx, col, counter, org.Key)

	return
}

func createPatients(ctx context.Context, col *sqlitekv.Collection, cnt *sqlitekv.Counter, orgKey string) (err error) {
	for i := 0; i < 10; i++ {
		n, err := cnt.Next(ctx)
		if err != nil {
			return err
		}
		patient := &Patient{
			Key:  fmt.Sprintf("%s/p/%d", orgKey, n),
			Name: fmt.Sprintf("Patient %d", n),
			Age:  20 + i,
		}
		buf, err := json.Marshal(patient)
		if err != nil {
			return err
		}

		patient.RID, err = col.Put(ctx, patient.Key, buf)
		if err != nil {
			return err
		}

		err = createRx(ctx, col, cnt, patient.Key)
		if err != nil {
			return err
		}
	}

	return
}

func createRx(ctx context.Context, col *sqlitekv.Collection, cnt *sqlitekv.Counter, patientKey string) (err error) {
	for i := 0; i < 5; i++ {
		n, err := cnt.Next(ctx)
		if err != nil {
			return err
		}
		rx := &Rx{
			Key:        fmt.Sprintf("%s/rx/%d", patientKey, n),
			Medication: fmt.Sprintf("Medication %d", n),
		}
		buf, err := json.Marshal(rx)
		if err != nil {
			return err
		}
		rx.RID, err = col.Put(ctx, rx.Key, buf)
		if err != nil {
			return err
		}
	}
	return
}
