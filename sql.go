package migrant

import "fmt"

type sqlDialect interface {
	createMeta() string
	insertMeta() string
	selectMeta() string
}

type universal struct{}

func (u *universal) createMeta() string {
	return fmt.Sprintf("CREATE TABLE %s (id SERIAL, migration TEXT NOT NULL UNIQUE, migrant_version TEXT NOT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);", metaTable)
}

func (u *universal) insertMeta() string {
	return fmt.Sprintf("INSERT INTO %s(migration, migrant_version, updated_at) VALUES($1, $2, NOW());", metaTable)
}

func (u *universal) selectMeta() string {
	return fmt.Sprintf("SELECT migration FROM %s ORDER BY id DESC;", metaTable)
}

func getDialect() sqlDialect {
	return &universal{}
}
