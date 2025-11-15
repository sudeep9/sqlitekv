package main

type Metadata struct {
	RowId int64  `json:"-"`
	Key   string `json:"-"`
	Its   int64  `json:"its"`
	Uts   int64  `json:"uts"`
	Cas   int64  `json:"cas"`
}

type Org struct {
	Meta Metadata `json:"_metadata"`
	Name string   `json:"name"`
}

type Patient struct {
	Meta  Metadata `json:"_metadata"`
	Name  string   `json:"name"`
	Phone string   `json:"phone"`
	Age   int      `json:"age"`
}

type Rx struct {
	Meta       Metadata `json:"_metadata"`
	Medication string   `json:"meds"`
}
