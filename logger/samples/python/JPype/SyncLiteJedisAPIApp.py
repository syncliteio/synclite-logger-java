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
Jedis API sample via JPype.

This sample demonstrates a broader Redis-style command surface on top of the
SyncLite-backed Jedis wrapper, including strings, hashes, lists, sets, sorted
sets, and key lifecycle operations.

The sample uses managed builder mode, so SQLiteStore initialize/open/close is
handled by Jedis itself.
"""

from _common import start_jvm


def main():
    start_jvm()

    from java.nio.file import Path
    from java.util import HashMap
    from io.synclite.logger import Jedis

    db_path = Path.of("sample_jedis_store_jpype.db")

    jedis = Jedis.builder(db_path, Path.of("synclite_logger.conf"), "jedis-sample-jpype") \
        .host("localhost") \
        .port(6379) \
        .build()

    try:
        # Clear keys used by this sample so repeated runs stay deterministic.
        jedis.del(
            "sample:user:1:name",
            "sample:user:2:name",
            "sample:user:3:name",
            "sample:session:42",
            "sample:queue",
            "sample:tags",
            "sample:leaderboard",
            "sample:tmp",
            "sample:tmp:renamed",
        )

        # 1) Strings
        jedis.set("sample:user:1:name", "Alice")
        jedis.mset("sample:user:2:name", "Bob", "sample:user:3:name", "Carol")
        print("GET sample:user:1:name =", jedis.get("sample:user:1:name"))
        print("MGET users =", jedis.mget("sample:user:2:name", "sample:user:3:name"))

        # 2) Hashes
        session = HashMap()
        session.put("token", "abc123")
        session.put("status", "active")
        session.put("region", "us-east")
        jedis.hset("sample:session:42", session)
        print("HGET token =", jedis.hget("sample:session:42", "token"))
        print("HGETALL session =", jedis.hgetAll("sample:session:42"))
        jedis.hdel("sample:session:42", "region")

        # 3) Lists
        jedis.rpush("sample:queue", "job-1", "job-2")
        jedis.lpush("sample:queue", "job-0")
        print("LRANGE queue =", jedis.lrange("sample:queue", 0, -1))
        print("LPOP queue =", jedis.lpop("sample:queue"))
        print("RPOP queue =", jedis.rpop("sample:queue"))

        # 4) Sets
        jedis.sadd("sample:tags", "etl", "cdc", "ops")
        jedis.srem("sample:tags", "ops")
        print("SMEMBERS tags =", jedis.smembers("sample:tags"))

        # 5) Sorted sets
        jedis.zadd("sample:leaderboard", 9.5, "alice")
        jedis.zadd("sample:leaderboard", 8.0, "bob")
        jedis.zadd("sample:leaderboard", 9.8, "carol")
        jedis.zincrby("sample:leaderboard", 0.3, "bob")
        print("ZRANGE leaderboard =", jedis.zrange("sample:leaderboard", 0, -1))
        print("ZSCORE bob =", jedis.zscore("sample:leaderboard", "bob"))

        # 6) Key lifecycle
        jedis.set("sample:tmp", "ephemeral")
        jedis.expire("sample:tmp", 60)
        print("TTL sample:tmp =", jedis.ttl("sample:tmp"))
        jedis.rename("sample:tmp", "sample:tmp:renamed")
        jedis.persist("sample:tmp:renamed")
        jedis.del("sample:tmp:renamed")

    finally:
        jedis.close()


if __name__ == "__main__":
    main()