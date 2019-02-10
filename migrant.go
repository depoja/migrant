package migrant

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
)

const version = "0.0.1"

// Migrant is the migration helper definition
type Migrant struct {
	db *sql.DB
}

// New creates a new migration helper for the given sql database
func New(db *sql.DB) *Migrant {
	return &Migrant{db}
}

func (migrant *Migrant) initMeta() {
	migrant.db.Exec("CREATE TABLE migration_meta (id serial primary key, migration text not null unique, migrant_version text not null, updated_at timestamp default current_timestamp);")
}

func (migrant *Migrant) setMeta(migrations []string) {
	for _, m := range migrations {

		result, err := migrant.db.Exec("INSERT INTO migration_meta(updated_at, migration, migrant_version) VALUES(NOW(), $1, $2);", m, version)
		if err != nil {
			log.Printf("DB Err: %s %s", err, result)
		}
	}
}

func (migrant *Migrant) getMeta() []string {
	var migrations []string

	rows, err := migrant.db.Query("SELECT migration FROM migration_meta ORDER BY id DESC;")
	if err != nil {
		log.Printf("DB Error: %s", err)
		return migrations
	}

	defer rows.Close()
	for rows.Next() {
		var migration string

		if err := rows.Scan(&migration); err == nil {
			migrations = append(migrations, migration)
		} else {
			log.Printf("DB Error: %s", err)
			return migrations
		}
	}
	return migrations
}

// Migrate executes all the unexecuted sql files (migrations) at the given path
func (migrant *Migrant) Migrate(path string) error {
	migrant.initMeta()
	dir := stripLast(path, '/')

	files, err := filepath.Glob(fmt.Sprintf("%s/*.sql", dir))
	if err != nil {
		return err
	}

	// Sort the files in alphabetical order (by timestamp)
	sort.Strings(files)

	migrations := migrant.getMeta()
	var completed []string

	for _, file := range files {
		filename := filepath.Base(file)

		if !contains(filename, migrations) {
			log.Printf("Migrating: %s", filename)

			if contents, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", dir, filename)); err == nil {
				if _, err := migrant.db.Exec(string(contents)); err == nil {
					log.Printf("Migrated: %s", filename)
					completed = append(completed, filename)
				} else {
					log.Printf("Could not migrate: %s", err)
					return err
				}
			} else {
				log.Printf("Could not migrate: %s", err)
				return err
			}

			if len(completed) > 0 {
				migrant.setMeta(completed)
				log.Printf("Completed: migrated up to %s\n", completed[len(completed)-1])
			} else {
				log.Println("Completed: no new migrations")
			}
		}
	}

	return nil
}
