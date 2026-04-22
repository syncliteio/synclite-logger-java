"""
Store API sample via JPype.

This sample uses the Store API rather than issuing raw SQL strings for every
change. It targets the same store-device model as the JDBC store sample:
mutable operations whose row data is explicit enough to be applied directly to
downstream destinations.

The contrast with a SQL device is semantic, not just syntactic. A SQL device
can log arbitrary SQL and rely on replay plus CDC extraction later. The Store
API is intentionally centered on direct CRUD-style changes, so patterns such as
INSERT INTO ... SELECT ... are outside its core model. Column drop is still
shown through JDBC DDL because Store API has no direct drop-column helper.
"""

from _common import start_jvm


def main():
    start_jvm()

    from java.nio.file import Path
    from java.sql import DriverManager
    from java.util import LinkedHashMap, HashMap, ArrayList
    from io.synclite.logger import SQLiteStore

    db_path = Path.of("sample_store_api_jpype.db")
    SQLiteStore.initialize(db_path, Path.of("synclite_logger.conf"))

    store = SQLiteStore.open(db_path)

    cols = LinkedHashMap()
    cols.put("id", "INTEGER PRIMARY KEY")
    cols.put("name", "TEXT")
    cols.put("score", "INTEGER")
    store.createTable("players", cols)

    archive_cols = LinkedHashMap()
    archive_cols.put("id", "INTEGER")
    archive_cols.put("note", "TEXT")
    store.createTable("temp_players_archive", archive_cols)

    row1 = HashMap()
    row1.put("id", 1)
    row1.put("name", "Alice")
    row1.put("score", 100)
    store.insert("players", row1)

    set_vals = HashMap()
    set_vals.put("score", 250)
    where_vals = HashMap()
    where_vals.put("id", 1)
    store.update("players", set_vals, where_vals)

    set_email = HashMap()
    set_email.put("email", "alice@example.com")
    store.update("players", set_email, where_vals)

    # Store API does not provide an explicit drop-column method.
    conn = DriverManager.getConnection("jdbc:synclite_sqlite_store:sample_store_api_jpype.db")
    stmt = conn.createStatement()
    stmt.execute("ALTER TABLE players DROP COLUMN email")
    stmt.close()
    conn.close()

    store.dropTable("temp_players_archive")
    rows = store.selectAll("players")
    print("Store API rows count:", rows.size())
    store.close()
    SQLiteStore.closeDevice(db_path)


if __name__ == "__main__":
    main()
