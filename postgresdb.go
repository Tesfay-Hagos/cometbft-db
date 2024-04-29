// Go
// TODO: make the iterator to handle none end and none start either of them to be null
package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type PostgresDB struct {
	db *pgx.Conn
}

var _ DB = (*PostgresDB)(nil)

func NewPostgresDB(name, dir string) (*PostgresDB, error) {
	connStr := fmt.Sprintf("postgresql://%s/%s?sslmode=disable", name, dir)
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return nil, err
	}

	// Create table if it doesn't exist
	createTableSQL := `CREATE TABLE IF NOT EXISTS kv_store (
		id SERIAL PRIMARY KEY,
        key BYTEA unique NOT NULL,
        value BYTEA NOT NULL
    );`

	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		return nil, err
	}

	return &PostgresDB{
		db: conn,
	}, nil
}

func (db *PostgresDB) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, errKeyEmpty
	}
	query := "SELECT value FROM kv_store WHERE key = $1"
	var value []byte
	err := db.db.QueryRow(context.Background(), query, key).Scan(&value)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (db *PostgresDB) Has(key []byte) (bool, error) {
	if key == nil {
		return false, errKeyEmpty
	}
	query := "SELECT exists (SELECT 1 FROM kv_store WHERE key = $1)"
	var exists bool
	err := db.db.QueryRow(context.Background(), query, key).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (db *PostgresDB) Set(key []byte, value []byte) error {
	if key == nil {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	query := "INSERT INTO kv_store(key, value) VALUES($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2"
	_, err := db.db.Exec(context.Background(), query, key, value)
	return err
}

func (db *PostgresDB) SetSync(key []byte, value []byte) error {
	if key == nil {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	tx, err := db.db.Begin(context.Background())
	if err != nil {
		return err
	}
	query := "INSERT INTO kv_store(key, value) VALUES($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2"
	_, err = tx.Exec(context.Background(), query, key, value)
	if err != nil {
		tx.Rollback(context.Background())
		return err
	}
	return tx.Commit(context.Background())
}

func (db *PostgresDB) Delete(key []byte) error {
	if key == nil {
		return errKeyEmpty
	}
	query := "DELETE FROM kv_store WHERE key = $1"
	_, err := db.db.Exec(context.Background(), query, key)
	return err
}

func (db *PostgresDB) DeleteSync(key []byte) error {
	if key == nil {
		return errKeyEmpty
	}

	tx, err := db.db.Begin(context.Background())
	if err != nil {
		return err
	}
	query := "DELETE FROM kv_store WHERE key = $1"
	_, err = tx.Exec(context.Background(), query, key)
	if err != nil {
		tx.Rollback(context.Background())
		return err
	}
	return tx.Commit(context.Background())
}

func (db *PostgresDB) Iterator(start, end []byte) (Iterator, error) {
	if len(start) == 0 {
		return nil, errors.New("start key cannot be empty")
	}
	if len(end) == 0 {
		return nil, errors.New("end key cannot be empty")
	}
	var startId, endId int
	if end == nil {
		return nil, errors.New("end key cannot be nil")
	}
	if start == nil {
		return nil, errors.New("start key cannot be nil")
	}
	err := db.db.QueryRow(context.Background(), "SELECT id FROM kv_store WHERE key = $1::bytea", start).Scan(&startId)
	if err != nil {
		return nil, err
	}
	err = db.db.QueryRow(context.Background(), "SELECT id FROM kv_store WHERE key = $1::bytea", end).Scan(&endId)
	if err != nil {
		return nil, err
	}
	query := "SELECT key, value FROM kv_store WHERE id >= $1 AND id < $2 ORDER BY id"
	rows, err := db.db.Query(context.Background(), query, startId, endId)
	if err != nil {
		return nil, err
	}
	return NewPostgresIterator(rows, start, end), nil
}
func (db *PostgresDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if len(start) == 0 {
		return nil, errors.New("start key cannot be empty")
	}
	if len(end) == 0 {
		return nil, errors.New("end key cannot be empty")
	}
	var startId, endId int
	if end == nil {
		return nil, errors.New("end key cannot be nil")
	}
	if start == nil {
		return nil, errors.New("start key cannot be nil")
	}
	err := db.db.QueryRow(context.Background(), "SELECT id FROM kv_store WHERE key = $1::bytea", start).Scan(&startId)
	if err != nil {
		return nil, err
	}
	err = db.db.QueryRow(context.Background(), "SELECT id FROM kv_store WHERE key = $1::bytea", end).Scan(&endId)
	if err != nil {
		return nil, err
	}
	query := "SELECT key, value FROM kv_store WHERE id >= $1 AND id < $2 ORDER BY id DESC"
	rows, err := db.db.Query(context.Background(), query, startId, endId)
	if err != nil {
		return nil, err
	}
	return NewPostgresIterator(rows, start, end), nil
}

func (db *PostgresDB) Close() error {
	return db.db.Close(context.Background())
}

func (db *PostgresDB) NewBatch() Batch {
	return NewPostgresDBBatch(db.db)
}

func (db *PostgresDB) Print() error {
	// Not applicable to PostgreSQL
	return nil
}

func (db *PostgresDB) Stats() map[string]string {
	// Not applicable to PostgreSQL
	return nil
}

func (db *PostgresDB) Compact(start, end []byte) error {
	// Not applicable to PostgreSQL
	return nil
}
