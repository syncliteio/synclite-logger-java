"""
Store device sample (JayDeBeApi + JDBC URL).

A store device is for mutable operational data where the row changes are
explicit in the operation itself. That lets SyncLite apply those changes
directly to downstream destinations instead of replaying the SQL on a replica
and deriving CDC afterward.

This is the core difference from a SQL device. A SQL device can log arbitrary
SQL and rely on replay plus CDC extraction later. A store device is narrower,
so patterns such as INSERT INTO target SELECT ... FROM source are not its core
model because the inserted rows are not already present in the request.
"""

import jaydebeapi

props = {
    "config": "synclite_logger.conf",
    "device-name": "store-device-sample"
}

conn = jaydebeapi.connect(
    "io.synclite.logger.SQLiteStore",
    "jdbc:synclite_sqlite_store:sample_store_sqlite_py.db",
    props,
    "synclite-logger-<version>.jar"
)

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS users(id INT PRIMARY KEY, name TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS temp_users_archive(id INT, note TEXT)")
# Full CRUD + schema evolution flow for a mutable table.
cur.executemany("INSERT INTO users VALUES(?, ?)", [[1, "Alice"], [2, "Bob"]])
cur.execute("UPDATE users SET name=? WHERE id=?", ["Alice Cooper", 1])
cur.execute("DELETE FROM users WHERE id=?", [2])
cur.execute("ALTER TABLE users ADD COLUMN email TEXT")
cur.execute("UPDATE users SET email=? WHERE id=?", ["alice@example.com", 1])
cur.execute("ALTER TABLE users DROP COLUMN email")
cur.execute("DROP TABLE IF EXISTS temp_users_archive")
cur.execute("SELECT id, name FROM users ORDER BY id")
print("Store Device rows:", cur.fetchall())
cur.execute("close database sample_store_sqlite_py.db")
cur.close()
conn.close()
