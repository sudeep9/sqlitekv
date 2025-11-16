package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sudeep9/sqlitekv"
)

type BaseColl struct {
	Id int64 `json:"-"`
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
	Phone     string `json:"phone"`
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
	})
	if err != nil {
		logger.Error("failed to create collection", slog.String("error", err.Error()))
		return
	}

	phone := "1234"

	p := &Person{}

	ok, err := col.GetUnique(ctx, "phone", phone, p)
	if err != nil {
		logger.Error("failed to get person by phone", slog.String("error", err.Error()))
		return
	}

	if !ok {
		logger.Info("person not found, creating new one", slog.String("phone", phone))
		p.FirstName = "John"
		p.LastName = "Doe"
		p.Phone = phone
		p.Age = 30
		err = col.Put(ctx, p)
		if err != nil {
			logger.Error("failed to put person", slog.String("error", err.Error()))
			return
		}
	}

	logger.Info("person retrieved", slog.Bool("found", ok), slog.Any("person", p))

	//err = createOrgs(ctx, st, 100)
	//if err != nil {
	//	logger.Error("failed to create orgs", slog.String("error", err.Error()))
	//	return
	//}

	/*
			maxCas := int64(0)
			st.patientCol.Select(ctx, func(fn sqlitekv.GetColumnValueFn) error {
				v, _, err := fn(0)
				if err != nil {
					return err
				}
				if v == nil {
					return nil
				}
				cas := v.(int64)
				if cas > maxCas {
					maxCas = cas
				}
				return nil
			}, sqlitekv.SelectOptions{
				Columns: []string{"cas"},
			})

			logger.Info("max cas", slog.Int64("maxCas", maxCas))
		logger.Info("kvtest completed", slog.Duration("duration", time.Since(now)))
	*/

}
