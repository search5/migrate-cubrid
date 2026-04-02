# migrate-cubrid

A [golang-migrate](https://github.com/golang-migrate/migrate) database driver for [CUBRID](https://www.cubrid.org/).

## Installation

```bash
go get github.com/search5/migrate-cubrid
```

## Usage

### With migrate URL

```go
import (
    "github.com/golang-migrate/migrate/v4"
    _ "github.com/search5/migrate-cubrid"
    _ "github.com/search5/cubrid-go"
)

m, err := migrate.New("file://migrations", "cubrid://dba:@localhost:33000/demodb")
if err != nil {
    log.Fatal(err)
}

// Apply all up migrations
if err := m.Up(); err != nil && err != migrate.ErrNoChange {
    log.Fatal(err)
}
```

### With existing database connection

```go
import (
    "database/sql"

    "github.com/golang-migrate/migrate/v4"
    migratecubrid "github.com/search5/migrate-cubrid"
    _ "github.com/search5/cubrid-go"
    "github.com/golang-migrate/migrate/v4/source/file"
)

db, err := sql.Open("cubrid", "cubrid://dba:@localhost:33000/demodb")
if err != nil {
    log.Fatal(err)
}

driver, err := migratecubrid.WithInstance(db, &migratecubrid.Config{
    DatabaseName: "demodb",
})
if err != nil {
    log.Fatal(err)
}

m, err := migrate.NewWithDatabaseInstance("file://migrations", "cubrid", driver)
if err != nil {
    log.Fatal(err)
}

m.Up()
```

## URL Query Parameters

| Parameter | Description | Default |
|---|---|---|
| `x-migrations-table` | Name of the migrations tracking table | `schema_migrations` |
| `x-no-lock` | Skip advisory locking (`true`/`false`) | `false` |
| `x-statement-timeout` | Timeout for migration statements in milliseconds | _(none)_ |

**Example:**

```
cubrid://dba:password@localhost:33000/demodb?x-migrations-table=my_migrations&x-statement-timeout=5000
```

## Migration Files

Create migration files in the standard golang-migrate format:

```
migrations/
├── 000001_create_users.up.sql
├── 000001_create_users.down.sql
├── 000002_add_email_column.up.sql
└── 000002_add_email_column.down.sql
```

Each file contains raw SQL statements for CUBRID:

```sql
-- 000001_create_users.up.sql
CREATE TABLE "users" (
    "id" INT AUTO_INCREMENT PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```sql
-- 000001_create_users.down.sql
DROP TABLE IF EXISTS "users";
```

## Features

- Full [database.Driver](https://pkg.go.dev/github.com/golang-migrate/migrate/v4/database#Driver) interface implementation
- Migration version tracking via configurable migrations table
- Table-based advisory locking to prevent concurrent migrations
- Dirty state detection and recovery
- Configurable statement timeout
- `Drop()` support for dropping all user tables

## Requirements

- Go 1.24.0+
- CUBRID 11.x
- [cubrid-go](https://github.com/search5/cubrid-go) driver

## License

MIT
