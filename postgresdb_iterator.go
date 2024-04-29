package db

import (
	"github.com/jackc/pgx/v5"
)

type PostgresIterator struct {
	source  pgx.Rows
	Items   item
	start   []byte
	end     []byte
	isValid bool
}

var _ Iterator = (*PostgresIterator)(nil)

// NewPostgresIterator creates a new PostgresIterator.
func NewPostgresIterator(source pgx.Rows, start, end []byte) *PostgresIterator {
	return &PostgresIterator{
		source:  source,
		Items:   item{key: nil, value: nil},
		isValid: true,
		start:   start,
		end:     end,
	}
}

// Domain implements Iterator.
func (itr *PostgresIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *PostgresIterator) Valid() bool {
	return itr.source != nil && itr.isValid
}

// Next implements Iterator.
func (itr *PostgresIterator) Next() {
	if itr.Valid() {
		if itr.source.Next() {
			itr.source.Scan(&itr.Items.key, &itr.Items.value)
		} else {
			itr.isValid = false
		}
	} else {
		panic("PostgresIterator is invalid")
	}
}

// Key implements Iterator.
func (itr *PostgresIterator) Key() []byte {
	if itr.Valid() {
		return itr.Items.key
	} else {
		panic("PostgresIterator is invalid")
	}
}

// Value implements Iterator.
func (itr *PostgresIterator) Value() []byte {
	if itr.Valid() {
		return itr.Items.value
	} else {
		panic("PostgresIterator is invalid")
	}
}

// Error implements Iterator.
func (itr *PostgresIterator) Error() error {
	if itr.source != nil {
		return itr.source.Err()
	}
	return nil
}

// Close implements Iterator.
func (itr *PostgresIterator) Close() error {
	if itr.source != nil {
		itr.source.Close()
	}
	return itr.Error()
}
