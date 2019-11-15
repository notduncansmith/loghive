package loghive

import (
	"path/filepath"

	"github.com/dgraph-io/badger"
	"github.com/notduncansmith/mutable"
)

// DBMap is a map of DB paths to open database handles
type DBMap = map[string]*badger.DB

// DBManager manages connections to Badger DBs
type DBManager struct {
	*mutable.RW
	DBMap
	Path string
}

// NewDBManager creates a new DBManager for a specified path
func NewDBManager(path string) *DBManager {
	return &DBManager{mutable.NewRW("DBManager"), DBMap{}, path}
}

// OpenDB opens a DB at a path, or returns one that has already been opened
func (m *DBManager) OpenDB(path string) (*badger.DB, error) {
	fullPath := filepath.Join(m.Path, path)
	var db *badger.DB

	m.DoWithRLock(func() {
		db = m.DBMap[fullPath]
	})

	if db != nil {
		return db, nil
	}

	db, err := badger.Open(badger.DefaultOptions(fullPath))

	if err != nil {
		return nil, errUnableToOpenFile(fullPath, err.Error())
	}

	m.DoWithRWLock(func() {
		m.DBMap[fullPath] = db
	})

	return db, nil
}
