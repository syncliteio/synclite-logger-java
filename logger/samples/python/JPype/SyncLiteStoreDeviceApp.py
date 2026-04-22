# Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
#
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.
#
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
