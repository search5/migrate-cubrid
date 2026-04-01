package cubrid

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
)

// --- Mock infrastructure ---

type mockResult struct{}

func (r mockResult) LastInsertId() (int64, error) { return 0, nil }
func (r mockResult) RowsAffected() (int64, error) { return 0, nil }

// mockTx implements txIface.
type mockTx struct {
	execErr   error
	execCount int
	failOnNth int // fail ExecContext on nth call (1-based), 0 = never
	commitErr error
	rollbackErr error
}

func (m *mockTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	m.execCount++
	if m.failOnNth > 0 && m.execCount == m.failOnNth {
		return nil, m.execErr
	}
	if m.failOnNth == 0 && m.execErr != nil {
		return nil, m.execErr
	}
	return mockResult{}, nil
}

func (m *mockTx) Rollback() error {
	return m.rollbackErr
}

func (m *mockTx) Commit() error {
	return m.commitErr
}

// mockConn implements connIface.
type mockConn struct {
	execErr    error
	queryErr   error
	beginTxErr error
	closeErr   error
	closed     bool
	tx         *mockTx
}

func (m *mockConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if m.execErr != nil {
		return nil, m.execErr
	}
	return mockResult{}, nil
}

func (m *mockConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return nil, nil
}

func (m *mockConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return nil
}

func (m *mockConn) beginTx(ctx context.Context, opts *sql.TxOptions) (txIface, error) {
	if m.beginTxErr != nil {
		return nil, m.beginTxErr
	}
	if m.tx != nil {
		return m.tx, nil
	}
	return &mockTx{}, nil
}

func (m *mockConn) Close() error {
	if m.closed {
		return errors.New("already closed")
	}
	m.closed = true
	return m.closeErr
}

// mockDB implements dbIface.
type mockDB struct {
	closeErr error
	closed   bool
}

func (m *mockDB) Ping() error                                 { return nil }
func (m *mockDB) QueryRow(query string, args ...any) *sql.Row { return nil }
func (m *mockDB) Conn(ctx context.Context) (*sql.Conn, error) { return nil, nil }
func (m *mockDB) Close() error {
	if m.closed {
		return errors.New("already closed")
	}
	m.closed = true
	return m.closeErr
}

type errReader struct{}

func (e *errReader) Read(p []byte) (int, error) {
	return 0, errors.New("read error")
}

// --- Unit tests ---

func TestOpen_InvalidURL(t *testing.T) {
	c := &Cubrid{}
	_, err := c.Open("://invalid")
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestOpen_InvalidNoLock(t *testing.T) {
	c := &Cubrid{}
	_, err := c.Open("cubrid://dba:@localhost:33000/demodb?x-no-lock=notabool")
	if err == nil {
		t.Fatal("expected error for invalid x-no-lock value")
	}
	if !strings.Contains(err.Error(), "invalid x-no-lock value") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_InvalidStatementTimeout(t *testing.T) {
	c := &Cubrid{}
	_, err := c.Open("cubrid://dba:@localhost:33000/demodb?x-statement-timeout=abc")
	if err == nil {
		t.Fatal("expected error for invalid x-statement-timeout")
	}
	if !strings.Contains(err.Error(), "invalid x-statement-timeout value") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithInstance_NilConfig(t *testing.T) {
	_, err := WithInstance(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestDefaultMigrationsTable(t *testing.T) {
	if DefaultMigrationsTable != "schema_migrations" {
		t.Fatalf("expected schema_migrations, got %s", DefaultMigrationsTable)
	}
}

func TestJava2GoDuration(t *testing.T) {
	d := java2GoDuration(5000)
	if d.Seconds() != 5 {
		t.Fatalf("expected 5s, got %v", d)
	}
}

func TestDriverRegistration(t *testing.T) {
	var _ database.Driver = (*Cubrid)(nil)
}

func TestConfig_Defaults(t *testing.T) {
	config := &Config{}
	if config.MigrationsTable != "" {
		t.Fatal("default MigrationsTable should be empty string")
	}
	if config.NoLock {
		t.Fatal("default NoLock should be false")
	}
	if config.StatementTimeout != 0 {
		t.Fatal("default StatementTimeout should be 0")
	}
}

func TestOpen_ParsesCustomParams(t *testing.T) {
	c := &Cubrid{}
	_, err := c.Open("cubrid://dba:@localhost:1/testdb?x-migrations-table=my_migrations&x-no-lock=true&x-statement-timeout=5000")
	if err == nil {
		t.Skip("unexpectedly connected to database")
	}
	errStr := err.Error()
	if strings.Contains(errStr, "invalid x-") {
		t.Fatalf("got parsing error instead of connection error: %v", err)
	}
}

// --- Close tests ---

func TestClose_ConnError(t *testing.T) {
	connErr := errors.New("conn close error")
	mc := &mockConn{closeErr: connErr}
	md := &mockDB{}
	c := &Cubrid{conn: mc, db: md, config: &Config{}}

	err := c.Close()
	if err != connErr {
		t.Fatalf("expected conn close error, got %v", err)
	}
}

func TestClose_DBError(t *testing.T) {
	mc := &mockConn{}
	dbErr := errors.New("db close error")
	md := &mockDB{closeErr: dbErr}
	c := &Cubrid{conn: mc, db: md, config: &Config{}}

	err := c.Close()
	if err != dbErr {
		t.Fatalf("expected db close error, got %v", err)
	}
}

func TestClose_BothOK(t *testing.T) {
	mc := &mockConn{}
	md := &mockDB{}
	c := &Cubrid{conn: mc, db: md, config: &Config{}}

	err := c.Close()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// --- ensureVersionTable / ensureLockTable tests ---

func TestEnsureVersionTable_Error(t *testing.T) {
	mc := &mockConn{execErr: errors.New("create table failed")}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.ensureVersionTable()
	if err == nil {
		t.Fatal("expected error")
	}
	dbErr, ok := err.(*database.Error)
	if !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
	if dbErr.Err != "failed to create migrations table" {
		t.Fatalf("unexpected error message: %s", dbErr.Err)
	}
}

func TestEnsureLockTable_Error(t *testing.T) {
	mc := &mockConn{execErr: errors.New("create lock table failed")}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.ensureLockTable()
	if err == nil {
		t.Fatal("expected error")
	}
	dbErr, ok := err.(*database.Error)
	if !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
	if dbErr.Err != "failed to create lock table" {
		t.Fatalf("unexpected error message: %s", dbErr.Err)
	}
}

func TestEnsureVersionTable_OK(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}
	if err := c.ensureVersionTable(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureLockTable_OK(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}
	if err := c.ensureLockTable(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Run tests ---

func TestRun_ReadError(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{}}

	err := c.Run(&errReader{})
	if err == nil {
		t.Fatal("expected error from bad reader")
	}
}

func TestRun_ExecError(t *testing.T) {
	mc := &mockConn{execErr: errors.New("exec failed")}
	c := &Cubrid{conn: mc, config: &Config{}}

	err := c.Run(strings.NewReader("SELECT 1"))
	if err == nil {
		t.Fatal("expected error")
	}
	dbErr, ok := err.(*database.Error)
	if !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
	if dbErr.Err != "migration failed" {
		t.Fatalf("unexpected error: %s", dbErr.Err)
	}
}

func TestRun_WithTimeout(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{StatementTimeout: 5000}}

	err := c.Run(strings.NewReader("SELECT 1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRun_OK(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{}}

	err := c.Run(strings.NewReader("CREATE TABLE test (id INT)"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- SetVersion tests ---

func TestSetVersion_BeginTxError(t *testing.T) {
	mc := &mockConn{beginTxErr: errors.New("begin tx failed")}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(1, false)
	if err == nil {
		t.Fatal("expected error")
	}
	dbErr, ok := err.(*database.Error)
	if !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
	if dbErr.Err != "transaction start failed" {
		t.Fatalf("unexpected error: %s", dbErr.Err)
	}
}

func TestSetVersion_DeleteError(t *testing.T) {
	mt := &mockTx{execErr: errors.New("delete failed"), failOnNth: 1}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(1, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(*database.Error); !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
}

func TestSetVersion_DeleteError_RollbackAlsoFails(t *testing.T) {
	mt := &mockTx{
		execErr:     errors.New("delete failed"),
		failOnNth:   1,
		rollbackErr: errors.New("rollback failed"),
	}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(1, false)
	if err == nil {
		t.Fatal("expected error")
	}
	// Should contain both errors via errors.Join
	errStr := err.Error()
	if !strings.Contains(errStr, "delete failed") {
		t.Fatalf("expected delete error in message: %s", errStr)
	}
}

func TestSetVersion_InsertError(t *testing.T) {
	mt := &mockTx{execErr: errors.New("insert failed"), failOnNth: 2}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(1, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(*database.Error); !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
}

func TestSetVersion_InsertError_RollbackAlsoFails(t *testing.T) {
	mt := &mockTx{
		execErr:     errors.New("insert failed"),
		failOnNth:   2,
		rollbackErr: errors.New("rollback failed"),
	}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(1, false)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSetVersion_CommitError(t *testing.T) {
	mt := &mockTx{commitErr: errors.New("commit failed")}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(1, false)
	if err == nil {
		t.Fatal("expected error")
	}
	dbErr, ok := err.(*database.Error)
	if !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
	if dbErr.Err != "transaction commit failed" {
		t.Fatalf("unexpected error: %s", dbErr.Err)
	}
}

func TestSetVersion_NilVersionNotDirty_Unit(t *testing.T) {
	mt := &mockTx{}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	// NilVersion, not dirty — should only DELETE, no INSERT
	err := c.SetVersion(database.NilVersion, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only 1 exec call (DELETE), no INSERT
	if mt.execCount != 1 {
		t.Fatalf("expected 1 exec call, got %d", mt.execCount)
	}
}

func TestSetVersion_NilVersionDirty_Unit(t *testing.T) {
	mt := &mockTx{}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	// NilVersion, dirty — should DELETE + INSERT
	err := c.SetVersion(database.NilVersion, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mt.execCount != 2 {
		t.Fatalf("expected 2 exec calls, got %d", mt.execCount)
	}
}

func TestSetVersion_PositiveVersion(t *testing.T) {
	mt := &mockTx{}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(42, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mt.execCount != 2 {
		t.Fatalf("expected 2 exec calls, got %d", mt.execCount)
	}
}

func TestSetVersion_PositiveVersionDirty(t *testing.T) {
	mt := &mockTx{}
	mc := &mockConn{tx: mt}
	c := &Cubrid{conn: mc, config: &Config{MigrationsTable: "test"}}

	err := c.SetVersion(42, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mt.execCount != 2 {
		t.Fatalf("expected 2 exec calls, got %d", mt.execCount)
	}
}

// --- Lock/Unlock tests ---

func TestLock_ExecError(t *testing.T) {
	mc := &mockConn{execErr: errors.New("insert failed")}
	c := &Cubrid{conn: mc, config: &Config{
		DatabaseName:    "testdb",
		MigrationsTable: "test",
	}}

	err := c.Lock()
	if err != database.ErrLocked {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
}

func TestUnlock_ExecError(t *testing.T) {
	mc := &mockConn{execErr: errors.New("delete failed")}
	c := &Cubrid{conn: mc, config: &Config{
		DatabaseName:    "testdb",
		MigrationsTable: "test",
	}}
	c.isLocked.Store(true)

	err := c.Unlock()
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(*database.Error); !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
}

func TestLock_NoLock(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{NoLock: true}}

	if err := c.Lock(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnlock_NoLock(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{NoLock: true}}
	c.isLocked.Store(true)

	if err := c.Unlock(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLock_OK(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{
		DatabaseName:    "testdb",
		MigrationsTable: "test",
	}}

	if err := c.Lock(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnlock_OK(t *testing.T) {
	mc := &mockConn{}
	c := &Cubrid{conn: mc, config: &Config{
		DatabaseName:    "testdb",
		MigrationsTable: "test",
	}}
	c.isLocked.Store(true)

	if err := c.Unlock(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Drop tests ---

func TestDrop_QueryError(t *testing.T) {
	mc := &mockConn{queryErr: errors.New("query failed")}
	c := &Cubrid{conn: mc, config: &Config{}}

	err := c.Drop()
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(*database.Error); !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
}
