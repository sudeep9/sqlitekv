package main

import (
	"github.com/sudeep9/sqlitekv"
)

type Metadata struct {
	Its int64 `json:"its"`
	Uts int64 `json:"uts"`
	Cas int64 `json:"cas"`
}

type BaseCollection struct {
	Id   int64    `json:"-"`
	Meta Metadata `json:"_m"`
}

func (o *BaseCollection) GetId() int64 {
	return o.Id
}
func (o *BaseCollection) SetId(id int64) {
	o.Id = id
}
func (o *BaseCollection) GetVal() (val []byte, err error) {
	return nil, sqlitekv.ErrUnimplemented
}
func (o *BaseCollection) SetVal(val []byte) error {
	return sqlitekv.ErrUnimplemented
}

func (o *BaseCollection) Column(i int, name string) (ok bool, val any, err error) {
	return false, nil, nil
}

func (o *BaseCollection) SetColumn(i int, name string, ok bool, val any) error {
	return nil
}

type Org struct {
	BaseCollection
	Name string `json:"name"`
}

func (o *Org) Column(i int, name string) (ok bool, val any, err error) {
	if name == "name" {
		return true, o.Name, nil
	}
	return false, nil, nil
}

func (o *Org) SetColumn(i int, name string, ok bool, val any) error {
	if name == "name" {
		if ok {
			o.Name = val.(string)
		}
	}
	return nil
}

type Patient struct {
	BaseCollection
	Oid   int64  `json:"oid"`
	Name  string `json:"name"`
	Phone string `json:"phone"`
	Age   int    `json:"age"`
}

type Rx struct {
	BaseCollection
	Pid        int64  `json:"pid"`
	Medication string `json:"meds"`
}
