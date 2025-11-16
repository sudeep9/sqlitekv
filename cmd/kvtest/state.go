package main

import (
	"context"
	"log/slog"

	"github.com/sudeep9/hlc"
	"github.com/sudeep9/sqlitekv"
)

type State struct {
	kv         *sqlitekv.KV
	logger     *slog.Logger
	hlc        *hlc.HLC
	counter    *sqlitekv.Counter
	genCol     *sqlitekv.Collection
	orgCol     *sqlitekv.Collection
	patientCol *sqlitekv.Collection
	rxCol      *sqlitekv.Collection
}

func newState(logger *slog.Logger, kv *sqlitekv.KV) (s *State, err error) {
	s = &State{
		kv:     kv,
		logger: logger,
		hlc:    hlc.NewHLC(0),
	}
	s.genCol, err = sqlitekv.NewCollection(kv, "gen", nil)
	if err != nil {
		return
	}

	s.counter, err = sqlitekv.NewCounter(s.genCol, sqlitekv.CounterOptions{})
	if err != nil {
		return
	}

	s.orgCol, err = sqlitekv.NewCollection(kv, "org", &sqlitekv.CollectionOptions{
		AutoId: true,
		Json:   true,
		Columns: []sqlitekv.DerivedColumn{
			{Name: "name", Type: "text"},
		},
	})
	if err != nil {
		return
	}

	s.patientCol, err = sqlitekv.NewCollection(kv, "patient", &sqlitekv.CollectionOptions{
		AutoId: true,
		Json:   true,
		FTS: &sqlitekv.FTSOptions{
			ExcludeKeys: []string{"_m"},
		},
	})
	if err != nil {
		return
	}

	s.rxCol, err = sqlitekv.NewCollection(kv, "rx", &sqlitekv.CollectionOptions{
		AutoId: true,
		Json:   true,
	})
	if err != nil {
		return
	}

	return
}

func (s *State) getMaxCas(ctx context.Context) (maxCas int64, err error) {
	var p Patient
	rowFn := func(pfn sqlitekv.ParseFn) error {
		err := pfn(&p)
		if err != nil {
			return err
		}

		if p.Meta.Cas > maxCas {
			maxCas = p.Meta.Cas
		}
		return nil
	}

	err = s.patientCol.Select(ctx, rowFn, sqlitekv.SelectOptions{})
	if err != nil {
		return
	}

	return
}

func (s *State) Cas() int64 {
	return s.hlc.Next()
}
