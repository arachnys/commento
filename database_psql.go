package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type PostgresDatabase struct {
	*sql.DB
}

func postgresInit(connectionStr string) (*PostgresDatabase, error) {
	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		return nil, err
	}

	statement := `
		CREATE TABLE IF NOT EXISTS comments (
			rowid serial primary key,
			url varchar(2083) not null,
			name varchar(200) not null,
			comment varchar(3000) not null,
			depth int not null,
			time timestamp not null,
			parent int
		);
	`
	if _, err = db.Exec(statement); err != nil {
		return nil, err
	} else {
		return &PostgresDatabase{db}, nil
	}
}

func (db *PostgresDatabase) CreateComment(c *Comment) error {
	statement := `
		SELECT depth, parent FROM comments WHERE rowid=$1;
	`
	rows, err := db.Query(statement, c.Parent)
	if err != nil {
		return err
	}
	defer rows.Close()

	depth := 0
	for rows.Next() {
		var pParent int
		if err := rows.Scan(&depth, &pParent); err == nil {
			if depth+1 > 5 {
				c.Parent = pParent
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Println(err)
		return err
	}

	statement = `
		INSERT INTO comments(url, name, comment, time, depth, parent) VALUES($1, $2, $3, $4, $5, $6);
	`
	_, err = db.Exec(statement, c.URL, c.Name, c.Comment, time.Now(), depth+1, c.Parent)
	return err
}

func (db *PostgresDatabase) GetComments(url string) ([]Comment, error) {
	statement := `
		SELECT rowid, url, comment, name, time, parent FROM comments WHERE url=$1;
	`
	rows, err := db.Query(statement, url)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	comments := []Comment{}
	for rows.Next() {
		c := Comment{}
		if err = rows.Scan(&c.ID, &c.URL, &c.Comment, &c.Name, &c.Timestamp, &c.Parent); err != nil {
			return nil, err
		}
		comments = append(comments, c)
	}

	return comments, rows.Err()
}
