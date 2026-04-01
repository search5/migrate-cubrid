module github.com/search5/migrate-cubrid

go 1.26.1

replace github.com/search5/cubrid-go => ../golang

require (
	github.com/golang-migrate/migrate/v4 v4.19.1
	github.com/search5/cubrid-go v0.0.0-00010101000000-000000000000
)
