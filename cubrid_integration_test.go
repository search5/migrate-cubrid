//go:build integration

package cubrid

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
	_ "github.com/search5/cubrid-go"
)

func integrationDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CUBRID_DSN")
	if dsn == "" {
		dsn = "cubrid://dba:@localhost:33000/demodb"
	}
	return dsn
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := integrationDSN(t)
	db, err := sql.Open("cubrid", dsn)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("cannot connect to CUBRID: %v", err)
	}
	return db
}

func cleanupTables(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tbl))
	}
}

func newTestDriver(t *testing.T, tableName string) (database.Driver, *sql.DB) {
	t.Helper()
	db := openTestDB(t)
	lockTable := tableName + "_lock"
	cleanupTables(t, db, tableName, lockTable)
	driver, err := WithInstance(db, &Config{
		MigrationsTable: tableName,
		DatabaseName:    "cubdb",
	})
	if err != nil {
		db.Close()
		t.Fatalf("WithInstance failed: %v", err)
	}
	return driver, db
}

func TestWithInstance_Integration(t *testing.T) {
	driver, _ := newTestDriver(t, "test_mig_wi")
	defer driver.Close()

	version, dirty, err := driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != database.NilVersion {
		t.Fatalf("expected NilVersion, got %d", version)
	}
	if dirty {
		t.Fatal("expected not dirty")
	}
}

func TestWithInstance_AutoDetectDBName(t *testing.T) {
	db := openTestDB(t)
	tableName := "test_mig_autoname"
	lockTable := tableName + "_lock"
	cleanupTables(t, db, tableName, lockTable)

	// DatabaseName left empty — should be auto-detected via SELECT database()
	driver, err := WithInstance(db, &Config{
		MigrationsTable: tableName,
	})
	if err != nil {
		db.Close()
		t.Fatalf("WithInstance with empty DatabaseName failed: %v", err)
	}
	driver.Close()

	db2 := openTestDB(t)
	defer db2.Close()
	cleanupTables(t, db2, tableName, lockTable)
}

func TestWithInstance_DefaultMigrationsTable(t *testing.T) {
	db := openTestDB(t)
	cleanupTables(t, db, DefaultMigrationsTable, DefaultMigrationsTable+"_lock")

	driver, err := WithInstance(db, &Config{
		DatabaseName: "cubdb",
	})
	if err != nil {
		db.Close()
		t.Fatalf("WithInstance with default table failed: %v", err)
	}
	driver.Close()

	db2 := openTestDB(t)
	defer db2.Close()
	cleanupTables(t, db2, DefaultMigrationsTable, DefaultMigrationsTable+"_lock")
}

func TestSetVersion_Integration(t *testing.T) {
	driver, _ := newTestDriver(t, "test_mig_sv")
	defer driver.Close()

	// Set version 1, not dirty
	if err := driver.SetVersion(1, false); err != nil {
		t.Fatalf("SetVersion(1, false) failed: %v", err)
	}
	version, dirty, err := driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != 1 || dirty {
		t.Fatalf("expected version=1 dirty=false, got version=%d dirty=%v", version, dirty)
	}

	// Set version 2, dirty
	if err := driver.SetVersion(2, true); err != nil {
		t.Fatalf("SetVersion(2, true) failed: %v", err)
	}
	version, dirty, err = driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != 2 || !dirty {
		t.Fatalf("expected version=2 dirty=true, got version=%d dirty=%v", version, dirty)
	}

	// Reset to NilVersion
	if err := driver.SetVersion(database.NilVersion, false); err != nil {
		t.Fatalf("SetVersion(NilVersion, false) failed: %v", err)
	}
	version, dirty, err = driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != database.NilVersion || dirty {
		t.Fatalf("expected NilVersion not dirty, got version=%d dirty=%v", version, dirty)
	}
}

func TestSetVersion_NilVersionDirty(t *testing.T) {
	driver, _ := newTestDriver(t, "test_mig_svnd")
	defer driver.Close()

	// Set NilVersion with dirty=true (dirty state without a real version)
	if err := driver.SetVersion(database.NilVersion, true); err != nil {
		t.Fatalf("SetVersion(NilVersion, true) failed: %v", err)
	}
	version, dirty, err := driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != database.NilVersion || !dirty {
		t.Fatalf("expected NilVersion dirty=true, got version=%d dirty=%v", version, dirty)
	}
}

func TestSetVersion_Overwrite(t *testing.T) {
	driver, _ := newTestDriver(t, "test_mig_svow")
	defer driver.Close()

	// Set version 5
	if err := driver.SetVersion(5, false); err != nil {
		t.Fatalf("SetVersion(5) failed: %v", err)
	}
	// Overwrite with version 10
	if err := driver.SetVersion(10, false); err != nil {
		t.Fatalf("SetVersion(10) failed: %v", err)
	}
	version, _, err := driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != 10 {
		t.Fatalf("expected 10, got %d", version)
	}
}

func TestLockUnlock_Integration(t *testing.T) {
	driver, _ := newTestDriver(t, "test_mig_lu")
	defer driver.Close()

	// Lock
	if err := driver.Lock(); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Double lock should fail
	if err := driver.Lock(); err != database.ErrLocked {
		t.Fatalf("expected ErrLocked, got %v", err)
	}

	// Unlock
	if err := driver.Unlock(); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Double unlock should fail
	if err := driver.Unlock(); err != database.ErrNotLocked {
		t.Fatalf("expected ErrNotLocked, got %v", err)
	}

	// Lock again should work
	if err := driver.Lock(); err != nil {
		t.Fatalf("second Lock failed: %v", err)
	}
	if err := driver.Unlock(); err != nil {
		t.Fatalf("second Unlock failed: %v", err)
	}
}

func TestNoLock_Integration(t *testing.T) {
	db := openTestDB(t)

	tableName := "test_mig_nl"
	cleanupTables(t, db, tableName)

	driver, err := WithInstance(db, &Config{
		MigrationsTable: tableName,
		DatabaseName:    "cubdb",
		NoLock:          true,
	})
	if err != nil {
		db.Close()
		t.Fatalf("WithInstance failed: %v", err)
	}
	defer driver.Close()

	// Lock/Unlock should be no-ops
	if err := driver.Lock(); err != nil {
		t.Fatalf("Lock with NoLock failed: %v", err)
	}
	if err := driver.Unlock(); err != nil {
		t.Fatalf("Unlock with NoLock failed: %v", err)
	}
}

func TestRun_Integration(t *testing.T) {
	driver, db := newTestDriver(t, "test_mig_run")
	defer driver.Close()
	testTable := "test_run_table"
	cleanupTables(t, db, testTable)

	migration := strings.NewReader(`CREATE TABLE "test_run_table" (id INT PRIMARY KEY, name VARCHAR(100))`)
	if err := driver.Run(migration); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	_, err := db.Exec(`INSERT INTO "test_run_table" (id, name) VALUES (1, 'hello')`)
	if err != nil {
		t.Fatalf("insert into test table failed: %v", err)
	}
}

func TestRun_InvalidSQL(t *testing.T) {
	driver, _ := newTestDriver(t, "test_mig_runerr")
	defer driver.Close()

	// Invalid SQL should return an error
	migration := strings.NewReader(`THIS IS NOT VALID SQL`)
	err := driver.Run(migration)
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
	dbErr, ok := err.(*database.Error)
	if !ok {
		t.Fatalf("expected *database.Error, got %T", err)
	}
	if dbErr.OrigErr == nil {
		t.Fatal("expected OrigErr to be set")
	}
}

func TestRun_WithStatementTimeout(t *testing.T) {
	db := openTestDB(t)
	tableName := "test_mig_runto"
	lockTable := tableName + "_lock"
	cleanupTables(t, db, tableName, lockTable, "test_timeout_tbl")

	driver, err := WithInstance(db, &Config{
		MigrationsTable:  tableName,
		DatabaseName:     "cubdb",
		StatementTimeout: 30000, // 30 seconds — generous timeout
	})
	if err != nil {
		db.Close()
		t.Fatalf("WithInstance failed: %v", err)
	}
	defer driver.Close()

	// Simple migration with timeout enabled
	migration := strings.NewReader(`CREATE TABLE "test_timeout_tbl" (id INT)`)
	if err := driver.Run(migration); err != nil {
		t.Fatalf("Run with timeout failed: %v", err)
	}
}

func TestDrop_Integration(t *testing.T) {
	db := openTestDB(t)

	tableName := "test_mig_drop"
	lockTable := tableName + "_lock"
	cleanupTables(t, db, tableName, lockTable, "test_drop_a", "test_drop_b")

	driver, err := WithInstance(db, &Config{
		MigrationsTable: tableName,
		DatabaseName:    "cubdb",
	})
	if err != nil {
		t.Fatalf("WithInstance failed: %v", err)
	}

	db.Exec(`CREATE TABLE "test_drop_a" (id INT)`)
	db.Exec(`CREATE TABLE "test_drop_b" (id INT)`)

	if err := driver.Drop(); err != nil {
		t.Fatalf("Drop failed: %v", err)
	}

	driver.Close()

	db2 := openTestDB(t)
	defer db2.Close()

	var count int
	err = db2.QueryRow(`SELECT COUNT(*) FROM db_class WHERE is_system_class = 'NO' AND class_type = 'CLASS' AND class_name IN ('test_drop_a', 'test_drop_b', '` + tableName + `', '` + lockTable + `')`).Scan(&count)
	if err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 tables remaining, got %d", count)
	}
}

func TestDrop_NoTables(t *testing.T) {
	db := openTestDB(t)

	tableName := "test_mig_dropempty"
	lockTable := tableName + "_lock"
	cleanupTables(t, db, tableName, lockTable)

	driver, err := WithInstance(db, &Config{
		MigrationsTable: tableName,
		DatabaseName:    "cubdb",
		NoLock:          true,
	})
	if err != nil {
		db.Close()
		t.Fatalf("WithInstance failed: %v", err)
	}

	// Drop all user tables first
	if err := driver.Drop(); err != nil {
		t.Fatalf("first Drop failed: %v", err)
	}

	// Drop again with no tables — should succeed (early return path)
	driver.Close()

	// Reopen since Drop is a breaking action
	db2 := openTestDB(t)
	driver2, err := WithInstance(db2, &Config{
		MigrationsTable: tableName,
		DatabaseName:    "cubdb",
		NoLock:          true,
	})
	if err != nil {
		db2.Close()
		t.Fatalf("WithInstance after drop failed: %v", err)
	}
	defer driver2.Close()

	if err := driver2.Drop(); err != nil {
		t.Fatalf("second Drop (empty) failed: %v", err)
	}
}

func TestOpen_Integration(t *testing.T) {
	dsn := integrationDSN(t)
	c := &Cubrid{}

	tableName := "test_mig_open"
	lockTable := tableName + "_lock"

	db := openTestDB(t)
	cleanupTables(t, db, tableName, lockTable)
	db.Close()

	driver, err := c.Open(dsn + "?x-migrations-table=" + tableName)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	version, dirty, err := driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != database.NilVersion || dirty {
		t.Fatalf("expected NilVersion not dirty, got %d %v", version, dirty)
	}

	driver.Close()

	db = openTestDB(t)
	defer db.Close()
	cleanupTables(t, db, tableName, lockTable)
}

func TestOpen_WithNoLock(t *testing.T) {
	dsn := integrationDSN(t)
	c := &Cubrid{}

	tableName := "test_mig_opennl"

	db := openTestDB(t)
	cleanupTables(t, db, tableName)
	db.Close()

	driver, err := c.Open(dsn + "?x-migrations-table=" + tableName + "&x-no-lock=true")
	if err != nil {
		t.Fatalf("Open with no-lock failed: %v", err)
	}

	version, _, err := driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != database.NilVersion {
		t.Fatalf("expected NilVersion, got %d", version)
	}

	driver.Close()

	db = openTestDB(t)
	defer db.Close()
	cleanupTables(t, db, tableName)
}

func TestOpen_WithStatementTimeout(t *testing.T) {
	dsn := integrationDSN(t)
	c := &Cubrid{}

	tableName := "test_mig_opento"
	lockTable := tableName + "_lock"

	db := openTestDB(t)
	cleanupTables(t, db, tableName, lockTable)
	db.Close()

	driver, err := c.Open(dsn + "?x-migrations-table=" + tableName + "&x-statement-timeout=5000")
	if err != nil {
		t.Fatalf("Open with statement-timeout failed: %v", err)
	}
	driver.Close()

	db = openTestDB(t)
	defer db.Close()
	cleanupTables(t, db, tableName, lockTable)
}

func TestFullMigrationWorkflow(t *testing.T) {
	driver, db := newTestDriver(t, "test_mig_workflow")
	defer driver.Close()
	cleanupTables(t, db, "test_wf_users")

	// Simulate migrate's workflow: lock -> setVersion dirty -> run -> setVersion clean -> unlock
	if err := driver.Lock(); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	if err := driver.SetVersion(1, true); err != nil {
		t.Fatalf("SetVersion(1, true) failed: %v", err)
	}

	migration := strings.NewReader(`CREATE TABLE "test_wf_users" (id INT PRIMARY KEY, email VARCHAR(255))`)
	if err := driver.Run(migration); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if err := driver.SetVersion(1, false); err != nil {
		t.Fatalf("SetVersion(1, false) failed: %v", err)
	}

	if err := driver.Unlock(); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	version, dirty, err := driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != 1 || dirty {
		t.Fatalf("expected version=1 dirty=false, got version=%d dirty=%v", version, dirty)
	}

	// Second migration
	if err := driver.Lock(); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	if err := driver.SetVersion(2, true); err != nil {
		t.Fatalf("SetVersion(2, true) failed: %v", err)
	}

	migration2 := strings.NewReader(`ALTER TABLE "test_wf_users" ADD COLUMN "name" VARCHAR(100)`)
	if err := driver.Run(migration2); err != nil {
		t.Fatalf("Run migration 2 failed: %v", err)
	}

	if err := driver.SetVersion(2, false); err != nil {
		t.Fatalf("SetVersion(2, false) failed: %v", err)
	}

	if err := driver.Unlock(); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	version, dirty, err = driver.Version()
	if err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if version != 2 || dirty {
		t.Fatalf("expected version=2 dirty=false, got version=%d dirty=%v", version, dirty)
	}
}

func TestClose_Integration(t *testing.T) {
	driver, _ := newTestDriver(t, "test_mig_close")

	// First close should succeed
	if err := driver.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Second close should return error (already closed)
	if err := driver.Close(); err == nil {
		t.Fatal("expected error on second Close")
	}
}
