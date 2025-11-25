package sqlitekv

import (
	"database/sql"
	"sync"
)

type StmtStore struct {
	rw sync.RWMutex
	m  map[string]*sql.Stmt
}

func NewStmtStore() *StmtStore {
	return &StmtStore{
		m: make(map[string]*sql.Stmt),
	}
}

func (s *StmtStore) GetOrCreate(db *sql.DB, stmtName string, getSql func() string) (*sql.Stmt, error) {
	s.rw.RLock()
	stmt, ok := s.m[stmtName]
	s.rw.RUnlock()
	if ok {
		return stmt, nil
	}

	s.rw.Lock()
	defer s.rw.Unlock()
	stmt, err := db.Prepare(getSql())
	if err != nil {
		return nil, err
	}
	s.m[stmtName] = stmt
	return stmt, nil
}
