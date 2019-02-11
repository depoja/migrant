package migrant

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
)

var migrantVersion, metaTable = "0.0.1", "migration_meta"

// Migrant is the migration helper definition
type Migrant struct {
	db  *sql.DB
	tx  *sql.Tx
	sql sqlDialect
}

// New creates a new migration helper for the given sql database
func New(db *sql.DB) *Migrant {
	return &Migrant{db: db, sql: getDialect()}
}

// transaction creates a new transaction
func (m *Migrant) transaction() error {
	tx, err := m.db.Begin()
	m.tx = tx
	return err
}

// createMeta creates the migration metadata table (if not present)
func (m *Migrant) createMeta() error {
	if _, err := m.tx.Exec(m.sql.createMeta()); err != nil {
		return m.rollback(fmt.Errorf("Migrant - failed creating metadata table: %s", err))
	}
	return m.tx.Commit()
}

// insertMeta inserts the migration entries into the migration metadata table
func (m *Migrant) insertMeta(migrations []string) error {
	for _, migration := range migrations {
		if _, err := m.tx.Exec(m.sql.insertMeta(), migration, migrantVersion); err != nil {
			return err
		}
	}
	return nil
}

// selectMeta selects the migration entries from the migration metadata table
func (m *Migrant) selectMeta() ([]string, error) {
	var migrations []string

	rows, err := m.tx.Query(m.sql.selectMeta())
	if err != nil {
		return migrations, err
	}

	defer rows.Close()
	for rows.Next() {
		var migration string
		if err := rows.Scan(&migration); err != nil {
			return migrations, err
		}
		migrations = append(migrations, migration)
	}

	return migrations, nil
}

func (m *Migrant) rollback(err error) error {
	m.tx.Rollback()
	log.Println(err)
	return err
}

// Migrate executes all the unexecuted sql files (migrations) at the given path
func (m *Migrant) Migrate(path string) error {
	if err := m.transaction(); err == nil {
		m.createMeta()
	}

	if err := m.transaction(); err == nil {
		// Get all the migration files
		files, err := getFiles(path)
		if err != nil {
			return m.rollback(fmt.Errorf("Migrant - failed reading migration files: %s", err))
		}

		// Get the present migration entries
		migrations, _ := m.selectMeta()
		var completed []string
		for _, file := range files {
			filename := filepath.Base(file)

			// If migration has not been executed
			if !included(filename, migrations) {

				// Read the contents (query) of the migration file
				query, err := readFile(path, filename)
				if err != nil {
					return m.rollback(fmt.Errorf("Migrant - failed reading migration: %s %s", filename, err))
				}

				// Execture the migration file
				log.Printf("Migrant - running migration: %s\n", filename)
				if _, err := m.tx.Exec(query); err == nil {
					log.Printf("Migrant - completed migrating: %s\n", filename)
					completed = append(completed, filename)
				} else {
					return m.rollback(fmt.Errorf("Migrant - failed migrating: %s %s", filename, err))
				}

			}
		}

		// Update the migration metadata table
		if len(completed) > 0 {
			if err := m.insertMeta(completed); err != nil {
				return m.rollback(fmt.Errorf("Migrant - failed updating migration table: %s", err))
			}
		} else {
			log.Println("Migrant - no new migrations")
		}
		return m.tx.Commit()
	}
	return fmt.Errorf("Migrant - failed creating migration transaction")
}
