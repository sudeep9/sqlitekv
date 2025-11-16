package sqlitekv

import "errors"

const (
	SQLITE_CONSTRAINT_UNIQUE     = 2067
	SQLITE_CONSTRAINT_PRIMARYKEY = 1555
)

var (
	ErrUniqueConstraint  = errors.New("constraint failed")
	ErrPrimaryConstraint = errors.New("primary key constraint failed")
)
