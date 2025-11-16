package main

type Metadata struct {
	Its int64 `json:"its"`
	Uts int64 `json:"uts"`
	Cas int64 `json:"cas"`
}

type Org struct {
	Meta Metadata `json:"_m"`
	Name string   `json:"name"`
}

type Patient struct {
	Meta  Metadata `json:"_m"`
	Name  string   `json:"name"`
	Phone string   `json:"phone"`
	Age   int      `json:"age"`
}

type Rx struct {
	Meta       Metadata `json:"_m"`
	Medication string   `json:"meds"`
}
