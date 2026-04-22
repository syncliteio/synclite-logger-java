"""
SQL device sample via JPype.

A SQL device accepts general SQL and logs the statements as written. During
consolidation those statements are replayed on a replica, CDC is derived from
the replica, and that change stream is then applied to downstream destinations.

That makes SQL devices the right fit for full relational behavior, including
statements such as INSERT INTO ... SELECT ... FROM .... Store devices are
narrower and focus on operations where the changed data is already explicit in
the request.
"""

from _common import start_jvm


def main():
    start_jvm()

    from java.nio.file import Path
    from java.sql import DriverManager
    from io.synclite.logger import SQLite

    db_path = Path.of("sample_txn_sqlite_jpype.db")
    SQLite.initialize(db_path, Path.of("synclite_logger.conf"))

    conn = DriverManager.getConnection("jdbc:synclite_sqlite:sample_txn_sqlite_jpype.db")
    stmt = conn.createStatement()
    stmt.execute("CREATE TABLE IF NOT EXISTS feedback(rating INT, comment TEXT)")
    stmt.execute("CREATE TABLE IF NOT EXISTS temp_feedback_archive(id INT, note TEXT)")
    stmt.execute("INSERT INTO feedback VALUES(3, 'Good product')")
    # SQL device transaction control is explicit and familiar.
    conn.setAutoCommit(False)
    stmt.execute("UPDATE feedback SET comment='Better product' WHERE rating=3")
    stmt.execute("INSERT INTO feedback VALUES(1, 'Poor product')")
    stmt.execute("DELETE FROM feedback WHERE rating=1")
    conn.commit()
    conn.setAutoCommit(True)
    stmt.execute("ALTER TABLE feedback ADD COLUMN source TEXT")
    stmt.execute("UPDATE feedback SET source='web' WHERE rating=3")
    stmt.execute("ALTER TABLE feedback DROP COLUMN source")
    stmt.execute("DROP TABLE IF EXISTS temp_feedback_archive")
    stmt.close()
    conn.close()
    SQLite.closeDevice(db_path)


if __name__ == "__main__":
    main()
