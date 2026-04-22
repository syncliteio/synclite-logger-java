"""
Store device sample via JPype and JDBC SQL.

A store device is for mutable operational data where the row changes are
explicit in the operation itself. That lets SyncLite apply those changes
directly to downstream destinations instead of replaying the SQL on a replica
and deriving CDC afterward.

This is the core difference from a SQL device. A SQL device can log arbitrary
SQL and rely on replay plus CDC extraction later. A store device is narrower,
so patterns such as INSERT INTO target SELECT ... FROM source are not its core
model because the inserted rows are not already present in the request.


def main():
    start_jvm()

    from java.nio.file import Path
    from java.sql import DriverManager
    from io.synclite.logger import SQLiteStore

    db_path = Path.of("sample_store_sqlite_jpype.db")
    SQLiteStore.initialize(db_path, Path.of("synclite_logger.conf"))

    conn = DriverManager.getConnection("jdbc:synclite_sqlite_store:sample_store_sqlite_jpype.db")
    stmt = conn.createStatement()
    stmt.execute("CREATE TABLE IF NOT EXISTS users(id INT PRIMARY KEY, name TEXT)")
    stmt.execute("CREATE TABLE IF NOT EXISTS temp_users_archive(id INT, note TEXT)")
    # CRUD + schema evolution on mutable data.
    stmt.execute("INSERT INTO users VALUES(1, 'Alice')")
    stmt.execute("INSERT INTO users VALUES(2, 'Bob')")
    stmt.execute("UPDATE users SET name='Alice Cooper' WHERE id=1")
    stmt.execute("DELETE FROM users WHERE id=2")
    stmt.execute("ALTER TABLE users ADD COLUMN email TEXT")
    stmt.execute("UPDATE users SET email='alice@example.com' WHERE id=1")
    stmt.execute("ALTER TABLE users DROP COLUMN email")
    stmt.execute("DROP TABLE IF EXISTS temp_users_archive")
    stmt.close()
    conn.close()
    SQLiteStore.closeDevice(db_path)


if __name__ == "__main__":
    main()
