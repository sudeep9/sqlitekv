package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sudeep9/sqlitekv"
)

type BaseColl struct {
	Id   int64    `json:"-"`
	Meta Metadata `json:"_m"`
}

func (jc *BaseColl) GetId() int64 {
	return jc.Id
}
func (jc *BaseColl) SetId(id int64) {
	jc.Id = id
}
func (jc *BaseColl) GetVal() (val []byte, err error) {
	return nil, fmt.Errorf("unimplemented")
}
func (jc *BaseColl) SetVal(val []byte) error {
	return fmt.Errorf("unimplemented")
}
func (jc *BaseColl) Column(i int, name string) (ok bool, val any, err error) {
	return false, nil, fmt.Errorf("unimplemented")
}
func (jc *BaseColl) SetColumn(i int, name string, ok bool, val any) error {
	return fmt.Errorf("unimplemented")
}

type Person struct {
	BaseColl
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

	col, err := sqlitekv.NewCollection(kv, "gen", &sqlitekv.CollectionOptions{
		AutoId: true,
		Json:   true,
		Columns: []sqlitekv.DerivedColumn{
			{Name: "phone", Type: "text", Unique: true},
		},
		FTS: &sqlitekv.FTSOptions{
			ExcludeKeys: []string{"_m"},
			Columns:     []string{"phone"},
		},
	})
	if err != nil {
		logger.Error("failed to create collection", slog.String("error", err.Error()))
		return
	}

	plist := []*Person{}

	err = col.Search(ctx, "john", sqlitekv.GetAccumulateFn(&plist))
	if err != nil {
		logger.Error("failed to search persons", slog.String("error", err.Error()))
		return
	}

	for _, p := range plist {
		fmt.Printf("Person: %+v\n", p)
	}

	plist = []*Person{
		{FirstName: "John", LastName: "Doe", Phone: "123-456-7890", Age: 30},
		{FirstName: "Jane", LastName: "Smith", Phone: "987-654-3210", Age: 25},
		{FirstName: "Alice", LastName: "Johnson", Phone: "555-123-4567", Age: 28},
		{FirstName: "Bob", LastName: "Brown", Phone: "444-987-6543", Age: 35},
	}

	for _, p := range plist {
		_, err = col.Put(ctx, p)
		if err != nil {
			logger.Error("failed to put person", slog.String("error", err.Error()))
			return
		}
	}

}
