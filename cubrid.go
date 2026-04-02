// Package cubrid implements the migrate.database.Driver interface for CUBRID.
//
// Usage:
//
//	import (
//	    "github.com/golang-migrate/migrate/v4"
//	    _ "github.com/search5/migrate-cubrid"
//	    _ "github.com/search5/cubrid-go"
//	)
//
//	m, err := migrate.New("file://migrations", "cubrid://dba:@localhost:33000/demodb")
package cubrid

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	_ "github.com/search5/cubrid-go"
)

// Version is the current version of the migrate-cubrid driver.
const Version = "0.1.0"

func init() {
	database.Register("cubrid", &Cubrid{})
}

// DefaultMigrationsTable is the default name for the migrations tracking table.
const DefaultMigrationsTable = "schema_migrations"

// txIface abstracts *sql.Tx for testability.
type txIface interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Rollback() error
	Commit() error
}

// connIface abstracts *sql.Conn for testability.
type connIface interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	beginTx(ctx context.Context, opts *sql.TxOptions) (txIface, error)
	Close() error
}

// dbIface abstracts *sql.DB for testability.
type dbIface interface {
	Ping() error
	QueryRow(query string, args ...any) *sql.Row
	Conn(ctx context.Context) (*sql.Conn, error)
	Close() error
}

// sqlConnWrapper wraps *sql.Conn to implement connIface.
type sqlConnWrapper struct {
	*sql.Conn
}

func (w *sqlConnWrapper) beginTx(ctx context.Context, opts *sql.TxOptions) (txIface, error) {
	return w.Conn.BeginTx(ctx, opts)
}

// Config holds CUBRID-specific driver configuration.
type Config struct {
	// MigrationsTable is the name of the migrations tracking table.
	// Defaults to "schema_migrations".
	MigrationsTable string

	// DatabaseName is the name of the database (used for lock ID generation).
	DatabaseName string

	// NoLock skips advisory locking if true.
	NoLock bool

	// StatementTimeout is an optional timeout for migration statements in milliseconds.
	StatementTimeout int
}

// Cubrid implements database.Driver for CUBRID.
type Cubrid struct {
	conn     connIface
	db       dbIface
	isLocked atomic.Bool
	config   *Config
}

// WithInstance creates a new CUBRID migrate driver from an existing *sql.DB.
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, fmt.Errorf("config must not be nil")
	}
	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		var dbName string
		if err := instance.QueryRow("SELECT database()").Scan(&dbName); err != nil {
			return nil, &database.Error{OrigErr: err, Err: "failed to get database name"}
		}
		config.DatabaseName = dbName
	}

	if config.MigrationsTable == "" {
		config.MigrationsTable = DefaultMigrationsTable
	}

	ctx := context.Background()
	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	c := &Cubrid{
		conn:   &sqlConnWrapper{conn},
		db:     instance,
		config: config,
	}

	if err := c.ensureVersionTable(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	if !config.NoLock {
		if err := c.ensureLockTable(); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}

	return c, nil
}

// Open returns a new driver instance configured from a URL string.
func (c *Cubrid) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	config := &Config{
		DatabaseName: strings.TrimPrefix(purl.Path, "/"),
	}

	q := purl.Query()

	if s := q.Get("x-migrations-table"); s != "" {
		config.MigrationsTable = s
	} else {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if s := q.Get("x-no-lock"); s != "" {
		noLock, err := strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("invalid x-no-lock value: %w", err)
		}
		config.NoLock = noLock
	}

	if s := q.Get("x-statement-timeout"); s != "" {
		timeout, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("invalid x-statement-timeout value: %w", err)
		}
		config.StatementTimeout = timeout
	}

	// Remove custom params before passing to sql.Open
	q.Del("x-migrations-table")
	q.Del("x-no-lock")
	q.Del("x-statement-timeout")
	purl.RawQuery = q.Encode()

	db, err := sql.Open("cubrid", purl.String())
	if err != nil {
		return nil, err
	}

	return WithInstance(db, config)
}

// Close closes the database connection.
func (c *Cubrid) Close() error {
	connErr := c.conn.Close()
	dbErr := c.db.Close()
	if connErr != nil {
		return connErr
	}
	return dbErr
}

// Lock acquires a table-based advisory lock.
// CUBRID does not have built-in advisory locks, so we use a lock table.
func (c *Cubrid) Lock() error {
	return database.CasRestoreOnErr(&c.isLocked, false, true, database.ErrLocked, func() error {
		if c.config.NoLock {
			return nil
		}

		aid, err := database.GenerateAdvisoryLockId(c.config.DatabaseName, c.config.MigrationsTable)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(`INSERT INTO "%s_lock" (lock_id) VALUES (?)`, c.config.MigrationsTable)
		if _, err := c.conn.ExecContext(context.Background(), query, aid); err != nil {
			return database.ErrLocked
		}

		return nil
	})
}

// Unlock releases the table-based advisory lock.
func (c *Cubrid) Unlock() error {
	return database.CasRestoreOnErr(&c.isLocked, true, false, database.ErrNotLocked, func() error {
		if c.config.NoLock {
			return nil
		}

		aid, err := database.GenerateAdvisoryLockId(c.config.DatabaseName, c.config.MigrationsTable)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(`DELETE FROM "%s_lock" WHERE lock_id = ?`, c.config.MigrationsTable)
		if _, err := c.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		return nil
	})
}

// Run applies a migration to the database.
func (c *Cubrid) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if c.config.StatementTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, java2GoDuration(c.config.StatementTimeout))
		defer cancel()
	}

	query := string(migr)
	if _, err := c.conn.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

// SetVersion saves the migration version and dirty state.
func (c *Cubrid) SetVersion(version int, dirty bool) error {
	tx, err := c.conn.beginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := fmt.Sprintf(`DELETE FROM "%s"`, c.config.MigrationsTable)
	if _, err := tx.ExecContext(context.Background(), query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = errors.Join(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Only insert if there's a real version, or if dirty with NilVersion
	if version >= 0 || (version == database.NilVersion && dirty) {
		query = fmt.Sprintf(`INSERT INTO "%s" (version, dirty) VALUES (?, ?)`, c.config.MigrationsTable)
		dirtyInt := 0
		if dirty {
			dirtyInt = 1
		}
		if _, err := tx.ExecContext(context.Background(), query, version, dirtyInt); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = errors.Join(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

// Version returns the currently active migration version.
func (c *Cubrid) Version() (version int, dirty bool, err error) {
	query := fmt.Sprintf(`SELECT version, dirty FROM "%s" LIMIT 1`, c.config.MigrationsTable)
	var dirtyInt int
	err = c.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirtyInt)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	default:
		return version, dirtyInt != 0, nil
	}
}

// Drop deletes all tables in the database.
func (c *Cubrid) Drop() error {
	query := `SELECT class_name FROM db_class WHERE is_system_class = 'NO' AND class_type = 'CLASS'`
	rows, err := c.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(tableNames) == 0 {
		return nil
	}

	for _, t := range tableNames {
		query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, t)
		if _, err := c.conn.ExecContext(context.Background(), query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

// ensureVersionTable creates the migrations table if it doesn't exist.
func (c *Cubrid) ensureVersionTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (version BIGINT NOT NULL PRIMARY KEY, dirty SHORT NOT NULL)`, c.config.MigrationsTable)
	if _, err := c.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Err: "failed to create migrations table", Query: []byte(query)}
	}
	return nil
}

// ensureLockTable creates the lock table if it doesn't exist.
func (c *Cubrid) ensureLockTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s_lock" (lock_id BIGINT NOT NULL PRIMARY KEY)`, c.config.MigrationsTable)
	if _, err := c.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Err: "failed to create lock table", Query: []byte(query)}
	}
	return nil
}

// Compile-time interface check.
var _ database.Driver = (*Cubrid)(nil)

// java2GoDuration converts milliseconds to time.Duration.
func java2GoDuration(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
