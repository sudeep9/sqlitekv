package main

import (
	"github.com/sudeep9/hlc"
	"github.com/sudeep9/sqlitekv"
)

type State struct {
	kv         *sqlitekv.KV
	hlc        *hlc.HLC
	counter    *sqlitekv.Counter
	genCol     *sqlitekv.JsonCollection
	orgCol     *sqlitekv.JsonCollection
	patientCol *sqlitekv.JsonCollection
	rxCol      *sqlitekv.JsonCollection
}

func newState(kv *sqlitekv.KV) (s *State, err error) {
	s = &State{
		kv:  kv,
		hlc: hlc.NewHLC(),
	}
	s.genCol, err = kv.JsonCollection("gen", nil)
	if err != nil {
		return
	}

	s.orgCol, err = kv.JsonCollection("org", &sqlitekv.JsonCollectionOptions{
		Columns: []sqlitekv.GeneratedColumn{
			{Name: "name", Type: "text", Def: "json_extract(val, '$.name')", Storage: "Stored"},
		},
	})
	if err != nil {
		return
	}
	s.patientCol, err = kv.JsonCollection("patient", &sqlitekv.JsonCollectionOptions{
		Columns: []sqlitekv.GeneratedColumn{
			{Name: "name", Type: "text", Def: "json_extract(val, '$.name')", Storage: "Stored"},
			{Name: "phone", Type: "text", Def: "json_extract(val, '$.phone')", Storage: "Stored"},
		},
		Indexes: []string{"name", "phone"},
	})
	if err != nil {
		return
	}
	s.rxCol, err = kv.JsonCollection("rx", nil)
	if err != nil {
		return
	}

	s.counter, err = sqlitekv.NewCounter(s.genCol, "/counter", sqlitekv.CounterOptions{})
	return
}

func (s *State) Cas() int64 {
	return s.hlc.Next()
}
