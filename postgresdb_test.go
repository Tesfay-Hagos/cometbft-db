package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresDB_SetSync(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the SetSync method
	err = db.SetSync(key, value)
	require.NoError(t, err)

	// Verify that the value is set correctly
	result, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, result)
}

func TestPostgresDB_SetGet(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the Set method
	err = db.Set(key, value)
	require.NoError(t, err)

	// Call the Get method
	result, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, result)
}

func TestPostgresDB_Delete(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the Set method
	err = db.Set(key, value)
	require.NoError(t, err)

	// Call the Delete method
	err = db.Delete(key)
	require.NoError(t, err)

	// Verify that the key is deleted
	result, err := db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestPostgresDB_Has(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the Set method
	err = db.Set(key, value)
	require.NoError(t, err)

	// Call the Has method
	exists, err := db.Has(key)
	require.NoError(t, err)
	assert.True(t, exists)

	// Call the Has method with non-existent key
	exists, err = db.Has([]byte("non_existent_key"))
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestPostgresDB_Iterator(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	items := []item{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
	}
	for _, item := range items {
		err = db.Set(item.key, item.value)
		require.NoError(t, err)
	}
	// Call the Iterator method
	iter, err := db.Iterator(items[0].key, items[len(items)-1].key)
	require.NoError(t, err)
	defer iter.Close()

	// Iterate over the results and verify
	idx := 0
	for {
		if err := iter.Error(); err != nil {
			iter.Next()
			v := iter.Value()
			k := iter.Key()
			assert.Equal(t, string(items[idx].key), string(k))
			assert.Equal(t, string(items[idx].value), string(v))
			idx++
		} else {
			break
		}
	}
	require.NoError(t, iter.Error())

}
func TestPostgresDB_ReverseIterator(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	items := []item{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
	}
	for _, item := range items {
		err = db.Set(item.key, item.value)
		require.NoError(t, err)
	}
	// Call the Iterator method
	iter, err := db.ReverseIterator(items[0].key, items[len(items)-1].key)
	require.NoError(t, err)
	defer iter.Close()

	// Iterate over the results and verify
	idx := 3
	for {
		if err := iter.Error(); err != nil {
			iter.Next()
			v := iter.Value()
			k := iter.Key()
			assert.Equal(t, string(items[idx].key), string(k))
			assert.Equal(t, string(items[idx].value), string(v))
			idx--
		} else {
			break
		}
	}
	require.NoError(t, iter.Error())

}
func TestPostgresDB_BatchWrite(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	// Call the NewBatch method
	batch := db.NewBatch()

	// Verify that the returned batch is of type PostgresDBBatch
	_, ok := batch.(*postgresDBBatch)
	assert.True(t, ok)
	// Test the Set method of the batch
	items := []item{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
	}
	for _, item := range items {
		err = batch.Set(item.key, item.value)
		require.NoError(t, err)
	}

	// Test the Delete method of the batch
	err = batch.Delete(items[0].key)
	require.NoError(t, err)

	// Test the Write method of the batch
	err = batch.Write()
	require.NoError(t, err)

	// Test the WriteSync method of the batch
	err = batch.WriteSync()
	require.NoError(t, err)

	// Test the Close method of the batch
	err = batch.Close()
	require.NoError(t, err)
}
func TestPostgresDB_BatchWritesync(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)

	// Call the NewBatch method
	batch := db.NewBatch()

	// Verify that the returned batch is of type PostgresDBBatch
	_, ok := batch.(*postgresDBBatch)
	assert.True(t, ok)
	// Test the Set method of the batch
	items := []item{
		{key: []byte("keyb1"), value: []byte("value1")},
		{key: []byte("keyb2"), value: []byte("value2")},
		{key: []byte("keyb3"), value: []byte("value3")},
		{key: []byte("keyb4"), value: []byte("value4")},
	}
	for _, item := range items {
		err = batch.Set(item.key, item.value)
		require.NoError(t, err)
	}

	// Test the Delete method of the batch
	err = batch.Delete(items[0].key)
	require.NoError(t, err)
	// Test the WriteSync method of the batch
	err = batch.WriteSync()
	require.NoError(t, err)

	// Test the Close method of the batch
	err = batch.Close()
	require.NoError(t, err)
}
