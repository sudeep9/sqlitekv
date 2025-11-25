package main

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sudeep9/sqlitekv"
)

type Meta struct {
	Its int64 `json:"its"`
	Uts int64 `json:"uts"`
}

type User struct {
	Meta    Meta   `json:"_m"`
	Id      int64  `json:"id"`
	Oid     int64  `json:"oid"`
	Name    string `json:"name"`
	Dob     string `json:"dob"`
	Addr    string `json:"addr"`
	Email   string `json:"email"`
	Company string `json:"company"`
}

func process(logger *slog.Logger, db *sql.DB) (err error) {
	userCol, err := createUserCollection(db)
	if err != nil {
		return
	}

	userCount := 100000
	oid := int64(1001)

	now := time.Now()
	for i := 0; i < userCount; i++ {
		u := &User{
			Id:      int64(i + 1),
			Oid:     oid,
			Name:    gofakeit.Name(),
			Dob:     gofakeit.Date().Format("2006-01-02"),
			Addr:    gofakeit.Address().Address,
			Email:   gofakeit.Email(),
			Company: gofakeit.Company(),
		}

		err = userCol.Upsert(u)
		if err != nil {
			return
		}
	}
	insertEnd := time.Now()
	logger.Info("Inserted users", "count", userCount, "duration", insertEnd.Sub(now).String())

	ulist, err := userCol.Select(nil, sqlitekv.SelectOptions[User]{})
	if err != nil {
		return
	}
	end := time.Now()
	logger.Info("User count", "count", len(ulist), "duration", end.Sub(insertEnd).String())

	//err = userCol.Train(1000)
	//if err != nil {
	//	return
	//}

	return
}

func createUserCollection(db *sql.DB) (userCol *sqlitekv.KeyVal[User], err error) {
	dictCol, err := sqlitekv.NewDictCollection(db)
	if err != nil {
		return
	}

	enc := sqlitekv.NewEncoder(dictCol)

	userCol, err = sqlitekv.NewKeyVal(db, "user", sqlitekv.KeyValOptions[User]{
		Compression: true,
		UseDict:     true,
		Enc:         enc,
		OnInsert:    func(u *User) { u.Meta.Its = time.Now().Unix() },
		OnUpdate:    func(u *User) { u.Meta.Uts = time.Now().Unix() },
		KeyField: &sqlitekv.KeyValField[User]{
			Name:   "id",
			Type:   "INTEGER",
			Get:    func(u *User) any { return u.Id },
			GetPtr: func(u *User) any { return &u.Id },
		},
		Fields: []*sqlitekv.KeyValField[User]{
			{
				Name:   "oid",
				Type:   "integer",
				Get:    func(u *User) any { return u.Oid },
				GetPtr: func(u *User) any { return &u.Oid },
			},
		},
	})
	if err != nil {
		return
	}

	return
}

func main() {
	logger := slog.Default()
	db, err := sql.Open("sqlite3", "./tmp/test.db")
	if err != nil {
		logger.Error("Failed to open database", "error", err)
		return
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		logger.Error("Failed to set journal mode", "error", err)
		return
	}
	_, err = db.Exec("PRAGMA synchronous=NORMAL;")
	if err != nil {
		logger.Error("Failed to set synchronous mode", "error", err)
		return
	}

	err = process(logger, db)
	if err != nil {
		logger.Error("Process failed", "error", err)
		return
	}
}
