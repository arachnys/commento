package main

import (
	"strings"
)

type Database interface {
	CommentService
}

var db Database

// parseConnectionStr parses the given connectionStr and extracts two pieces
// of information: which database to use and the connection parameters for
// that database. For example, in sqlite3, a filename is sufficient. This will
// be encoded as:
//
//     connectionStr := "sqlite3:file=commento.sqlite3"
//
// Naturally, key=value pairs depend on the database in question. For MongoDB,
// this could be a URL. Multiple key=value pairs can be separated by a
// semicolon. To summarize, the canonical form of this strings is:
//
//     connectionStr := "database:key1=value1;key2=value2;key3=value3"
func parseConnectionStr(connectionStr string) (string, error) {
	dbPos := strings.Index(connectionStr, ":")
	if dbPos == -1 {
		return "", errorList["err.db.conf.separator.missing"]
	}
	dbName := strings.TrimSpace(connectionStr[:dbPos])

	return dbName, nil
}

func LoadDatabase(connectionStr string) error {
	dbName, err := parseConnectionStr(connectionStr)
	if err != nil {
		return err
	}

	db = nil
	err = errorList["err.db.unimplemented"]
	switch dbName {
	case "sqlite3":
		db, err = sqliteInit(connectionStr)
	case "postgres":
		db, err = postgresInit(connectionStr)
	}

	return err
}
