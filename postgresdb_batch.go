package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type postgresDBBatch struct {
	conn    *pgx.Conn
	queries map[string]string
}

var _ Batch = (*postgresDBBatch)(nil)

func NewPostgresDBBatch(conn *pgx.Conn) *postgresDBBatch {
	return &postgresDBBatch{
		conn:    conn,
		queries: make(map[string]string),
	}
}

// Set implements Batch.
func (b *postgresDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	if _, ok := b.queries[string(key)]; !ok {
		b.queries[string(key)] = string(value)
	} else {
		return fmt.Errorf("key %s already exists", key)
	}
	return nil
}

// Delete implements Batch.
func (b *postgresDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	delete(b.queries, string(key))
	return nil
}

// Write implements Batch.
func (b *postgresDBBatch) Write() error {
	batch := &pgx.Batch{}

	for key, value := range b.queries {
		query := "INSERT INTO kv_store (key, value) VALUES ($1, $2);"
		batch.Queue(query, key, value)
	}

	br := b.conn.SendBatch(context.Background(), batch)
	defer br.Close()

	_, err := br.Exec()
	if err != nil {
		return err
	}
	if err := br.Close(); err != nil {
		return err
	}
	return b.Close()
}

// WriteSync implements Batch.
func (b *postgresDBBatch) WriteSync() error {
	batch := &pgx.Batch{}

	for key, value := range b.queries {
		query := "INSERT INTO kv_store (key, value) VALUES ($1, $2);"
		batch.Queue(query, key, value)
	}

	br := b.conn.SendBatch(context.Background(), batch)
	defer br.Close()

	_, err := br.Exec()
	if err != nil {
		return err
	}
	err = br.Close()
	if err != nil {
		return err
	}
	return b.Close()
}

// Close is a no-op for PostgreSQL batch.
func (b *postgresDBBatch) Close() error {
	// Clear queries for reusability
	b.queries = make(map[string]string)
	return nil
}
