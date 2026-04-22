"""
Appender device sample via JPype.

Appender devices are aimed at write-heavy ingest workloads where the dominant
operation is appending new rows. They keep the calling pattern simple and focus
on durable INSERT-style logging.

Compared to a SQL device, this is intentionally narrower. If the workload needs
arbitrary SQL semantics and replay on a replica, use a SQL device instead.
"""

from _common import start_jvm


def main():
    start_jvm()

    from java.nio.file import Path
    from java.sql import DriverManager
    from io.synclite.logger import SQLiteAppender

    db_path = Path.of("sample_appender_sqlite_jpype.db")
    SQLiteAppender.initialize(db_path, Path.of("synclite_logger.conf"))

    conn = DriverManager.getConnection("jdbc:synclite_sqlite_appender:sample_appender_sqlite_jpype.db")
    stmt = conn.createStatement()
    stmt.execute("CREATE TABLE IF NOT EXISTS feedback(rating INT, comment TEXT)")
    stmt.execute("INSERT INTO feedback VALUES(4, 'Excellent Product')")
    stmt.execute("INSERT INTO feedback VALUES(5, 'Outstanding Product')")
    stmt.close()
    conn.close()
    SQLiteAppender.closeDevice(db_path)


if __name__ == "__main__":
    main()
