package main

import (
	"context"

	"github.com/sudeep9/hlc"
	"github.com/sudeep9/sqlitekv"
)

type State struct {
	kv         *sqlitekv.KV
	hlc        *hlc.HLC
	counter    *sqlitekv.Counter
	genCol     *sqlitekv.Collection
	orgCol     *sqlitekv.Collection
	patientCol *sqlitekv.Collection
	rxCol      *sqlitekv.Collection
}

func newState(kv *sqlitekv.KV) (s *State, err error) {
	s = &State{
		kv:  kv,
		hlc: hlc.NewHLC(0),
	}
	s.genCol, err = sqlitekv.NewCollection(kv, "gen", nil)
	if err != nil {
		return
	}

	s.counter, err = sqlitekv.NewCounter(s.genCol, sqlitekv.CounterOptions{})
	if err != nil {
		return
	}
	return
}

func (s *State) getMaxCas(ctx context.Context) (maxCas int64, err error) {

	type metadata struct {
		Meta struct {
			Cas int64 `json:"cas"`
		} `json:"_meta"`
	}

	err = s.patientCol.List(ctx, "/", func(id int64, key string, rawJson []byte, gencols []any) error {
		//var m metadata
		//err := json.Unmarshal(rawJson, &m)
		//if err != nil {
		//	return err
		//}

		cas := gencols[2].(int64)

		if cas > maxCas {
			maxCas = cas
		}
		return nil
	}, sqlitekv.ListOptions{All: true})

	if err != nil {
		return
	}

	return
}

func (s *State) Cas() int64 {
	return s.hlc.Next()
}
