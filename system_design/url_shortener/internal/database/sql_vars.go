package database

import _ "embed"

//go:embed create_schema.sql
var CreateSchemaSqlString string

//go:embed drop_schema.sql
var DropSchemaSqlString string
