/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package io.synclite.logger;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.fppt.jedismock.RedisServer;

/**
 * Integration test for {@link Jedis}.
 *
 * <p>Uses an in-process {@link RedisServer} (jedis-mock) so no external Redis
 * installation or Docker daemon is required.  The mock server is started once
 * for the whole test class and flushed between individual tests.
 */
class JedisTest {

    private static RedisServer mockRedis;
    private static String      redisHost;
    private static int         redisPort;

    @BeforeAll
    static void startMockRedis() throws Exception {
        mockRedis = RedisServer.newRedisServer();
        mockRedis.start();
        redisHost = mockRedis.getHost();
        redisPort = mockRedis.getBindPort();
    }

    @AfterAll
    static void stopMockRedis() throws Exception {
        if (mockRedis != null) {
            mockRedis.stop();
        }
    }

    private Path testDbPath;
    private Path testStageDir;
    private SyncLiteStore store;

    @BeforeEach
    void setUp() throws Exception {
        // Flush any state left by the previous test.
        try (redis.clients.jedis.Jedis flush = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
            flush.flushAll();
        }

        Path testHome = Path.of(System.getProperty("user.home"))
                .resolve("synclite").resolve("test").resolve("JedisTest");
        testDbPath   = testHome.resolve("db").resolve("test.db");
        testStageDir = testHome.resolve("stageDir");

        if (Files.exists(testHome)) {
            deleteRecursively(testHome);
        }
        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        Path configPath = testHome.resolve("synclite_logger.conf");
        Files.writeString(configPath,
                "local-data-stage-directory = " + testStageDir + "\ndestination-type = FS\n");

        Class.forName("io.synclite.logger.SQLiteStore");
        SQLiteStore.initialize(testDbPath, configPath, "jedistest");
        store = SQLiteStore.open(testDbPath);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void testSetAndGet() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("k1", "v1");
            jedis.set("k2", "v2");

            // Redis returns the value
            assertEquals("v1", jedis.get("k1"));
            assertEquals("v2", jedis.get("k2"));

            // Store has both rows
            List<Map<String, Object>> rows = store.selectAll(Jedis.STRINGS_TABLE);
            assertEquals(2, rows.size());

            List<Map<String, Object>> k1Rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "k1"));
            assertEquals(1, k1Rows.size());
            assertEquals("v1", k1Rows.get(0).get("value"));
        }
    }

    @Test
    void testSetOverwritesExistingKey() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("k1", "original");
            jedis.set("k1", "updated");

            assertEquals("updated", jedis.get("k1"));

            // Only one row in the store for k1
            List<Map<String, Object>> k1Rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "k1"));
            assertEquals(1, k1Rows.size());
            assertEquals("updated", k1Rows.get(0).get("value"));
        }
    }

    @Test
    void testDel() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("k1", "v1");
            jedis.set("k2", "v2");

            jedis.del("k1");

            assertNull(jedis.get("k1"), "Deleted key should return null from Redis");
            assertEquals("v2", jedis.get("k2"));

            // k1 gone from store; k2 still present
            assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "k1")).size());
            assertEquals(1, store.select(Jedis.STRINGS_TABLE, Map.of("key", "k2")).size());
        }
    }

    @Test
    void testMultiKeyDel() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("k1", "v1");
            jedis.set("k2", "v2");
            jedis.set("k3", "v3");

            jedis.del("k1", "k3");

            assertNull(jedis.get("k1"));
            assertNull(jedis.get("k3"));
            assertEquals("v2", jedis.get("k2"));

            assertEquals(1, store.selectAll(Jedis.STRINGS_TABLE).size());
        }
    }

    @Test
    void testHsetAndHget() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.hset("user:1", "name", "Alice");
            jedis.hset("user:1", "email", "alice@example.com");

            assertEquals("Alice",              jedis.hget("user:1", "name"));
            assertEquals("alice@example.com",  jedis.hget("user:1", "email"));

            // Store has two rows for hash user:1
            List<Map<String, Object>> hashRows = store.selectAll(Jedis.HASHES_TABLE);
            assertEquals(2, hashRows.size());
        }
    }

    @Test
    void testHsetMap() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.hset("session:42", Map.of("token", "abc123", "user", "bob"));

            assertEquals("abc123", jedis.hget("session:42", "token"));
            assertEquals("bob",    jedis.hget("session:42", "user"));

            assertEquals(2, store.select(Jedis.HASHES_TABLE, Map.of("hash_key", "session:42")).size());
        }
    }

    @Test
    void testHdel() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.hset("user:1", "name", "Alice");
            jedis.hset("user:1", "age", "30");

            jedis.hdel("user:1", "age");

            assertNull(jedis.hget("user:1", "age"), "Deleted hash field should be null");
            assertEquals("Alice", jedis.hget("user:1", "name"), "Other fields unaffected");

            // Only name left in store
            List<Map<String, Object>> remaining = store.select(Jedis.HASHES_TABLE, Map.of("hash_key", "user:1"));
            assertEquals(1, remaining.size());
            assertEquals("name", remaining.get(0).get("field"));
        }
    }

    @Test
    void testDelRemovesHashEntries() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.hset("user:1", "name", "Alice");
            jedis.hset("user:1", "role", "admin");

            jedis.del("user:1");

            // All hash rows for user:1 removed from store
            assertEquals(0, store.select(Jedis.HASHES_TABLE, Map.of("hash_key", "user:1")).size());
        }
    }

    @Test
    void testWarmUpRebuildsCacheFromStore() throws Exception {
        // Phase 1 — write data through Jedis so it lands in the store
        try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis1.set("rebuild:str", "hello");
            jedis1.hset("rebuild:hash", "field1", "world");
        }

        // Phase 2 — manually clear Redis (simulate a restart) using a raw Jedis client
        // that goes directly to Redis without touching the store.
        try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
            raw.del("rebuild:str");
            raw.del("rebuild:hash");
            assertNull(raw.get("rebuild:str"),           "Redis should be empty after manual del");
            assertNull(raw.hget("rebuild:hash", "field1"), "Redis should be empty after manual del");
        }

        // Phase 3 — create a new Jedis instance; warmUp() should restore from store
        try (Jedis jedis2 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            assertEquals("hello", jedis2.get("rebuild:str"),            "warmUp should restore string key");
            assertEquals("world", jedis2.hget("rebuild:hash", "field1"), "warmUp should restore hash field");
        }
    }

    /**
     * The core SyncLite-Jedis guarantee: after a full process restart —
     * store closed, Redis wiped — opening the same persisted store and
     * constructing a new {@link Jedis} instance must restore all five Redis
     * data types back into the (now-empty) cache via {@link Jedis#warmUp()}.
     *
     * <p>Test flow:
     * <ol>
     *   <li><b>Phase 1 – running:</b> write strings, hashes, lists, sets and
     *       sorted sets through the SyncLite Jedis.  Data lands in both Redis
     *       and the backing SQLite store.</li>
     *   <li><b>Phase 2 – shutdown:</b> close the store and call
     *       {@code closeAllDevices()} exactly as an application shutdown would.</li>
     *   <li><b>Phase 3 – Redis gone:</b> flush the entire Redis cache, simulating
     *       a Redis server restart or eviction.</li>
     *   <li><b>Phase 4 – restart:</b> re-open the <em>same</em> store DB
     *       (no {@code initialize()} call — the data already exists in the file)
     *       and build a new {@code Jedis} instance.  The constructor calls
     *       {@code warmUp()} which must reload every key into Redis.</li>
     *   <li><b>Assertions:</b> all five data types are verified to be present
     *       and correct in Redis using only Redis-side reads.</li>
     * </ol>
     */
    @Test
    void testRestartReloadsAllTypesFromStore() throws Exception {
        // ── Phase 1: running ────────────────────────────────────────────────
        try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            // Strings
            jedis1.set("rs:str",  "hello");
            jedis1.setex("rs:str:ttl", 3600L, "withttl");
            // Hash
            jedis1.hset("rs:hash", Map.of("field1", "v1", "field2", "v2"));
            // List — rpush so order is a, b, c
            jedis1.rpush("rs:list", "a", "b", "c");
            // Set
            jedis1.sadd("rs:set", "x", "y", "z");
            // Sorted set — low-to-high scores so zrange returns gold, silver, bronze
            jedis1.zadd("rs:zset", Map.of("gold", 1.0, "silver", 2.0, "bronze", 3.0));
        }

        // ── Phase 2: application shutdown ───────────────────────────────────
        store.close();
        store = null;
        SQLiteStore.closeAllDevices();
        Thread.sleep(200); // let Windows release any file handles

        // ── Phase 3: Redis restart (full cache wipe) ─────────────────────────
        try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
            raw.flushAll();
            // Confirm everything is gone before we recreate the Jedis instance
            assertNull(raw.get("rs:str"),                          "cache should be empty after flushAll");
            assertTrue(raw.hgetAll("rs:hash").isEmpty(),           "cache should be empty after flushAll");
            assertTrue(raw.lrange("rs:list", 0, -1).isEmpty(),    "cache should be empty after flushAll");
            assertTrue(raw.smembers("rs:set").isEmpty(),          "cache should be empty after flushAll");
            assertTrue(raw.zrange("rs:zset", 0, -1).isEmpty(),    "cache should be empty after flushAll");
        }

        // ── Phase 4: application restart ─────────────────────────────────────
        // Re-open the existing store DB — no initialize(), data already there.
        SyncLiteStore restartedStore = SQLiteStore.open(testDbPath);
        try (Jedis jedis2 = Jedis.builder(restartedStore).host(redisHost).port(redisPort).build()) {
            // warmUp() executed inside the constructor — assert Redis is fully rebuilt

            // Strings
            assertEquals("hello",   jedis2.get("rs:str"),           "String key restored after restart");
            assertEquals("withttl", jedis2.get("rs:str:ttl"),       "TTL string key restored after restart");

            // Hash
            assertEquals("v1",      jedis2.hget("rs:hash", "field1"), "Hash field1 restored");
            assertEquals("v2",      jedis2.hget("rs:hash", "field2"), "Hash field2 restored");

            // List — order must be preserved
            assertEquals(List.of("a", "b", "c"),
                    jedis2.lrange("rs:list", 0, -1),             "List restored in insertion order");

            // Set — all members present (order is undefined for sets)
            Set<String> restoredSet = jedis2.smembers("rs:set");
            assertTrue(restoredSet.containsAll(Set.of("x", "y", "z")), "All set members restored");

            // Sorted set — zrange returns members in ascending score order
            assertEquals(List.of("gold", "silver", "bronze"),
                    jedis2.zrange("rs:zset", 0, -1),             "ZSet restored in score order");
        } finally {
            restartedStore.close();
            // tearDown will call closeAllDevices(); store is already null so nothing double-closed
        }
    }

    @Test
    void testSetex() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.setex("ttl:key", 3600L, "ttlvalue");

            assertEquals("ttlvalue", jedis.get("ttl:key"));

            List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "ttl:key"));
            assertEquals(1, rows.size());
            long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
            assertTrue(expiresAt > System.currentTimeMillis(), "expires_at should be in the future");
        }
    }

    // -------------------------------------------------------------------------
    // List tests
    // -------------------------------------------------------------------------

    @Test
    void testRpushAndListStore() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.rpush("mylist", "a", "b", "c");

            // Redis holds the list
            List<String> redisValues = jedis.lrange("mylist", 0, -1);
            assertEquals(List.of("a", "b", "c"), redisValues);

            // Store has 3 rows for mylist
            List<Map<String, Object>> rows = store.select(Jedis.LISTS_TABLE, Map.of("key", "mylist"));
            assertEquals(3, rows.size());
        }
    }

    @Test
    void testLpush() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.rpush("llist", "x");
            jedis.lpush("llist", "y");      // y is now head

            List<String> redisValues = jedis.lrange("llist", 0, -1);
            assertEquals("y", redisValues.get(0), "lpush value should be at the head");

            // 2 rows in store
            assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "llist")).size());
        }
    }

    @Test
    void testListWarmUp() throws Exception {
        try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis1.rpush("wlist", "p", "q", "r");
        }

        // Clear Redis
        try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
            raw.del("wlist");
        }

        try (Jedis jedis2 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            List<String> restored = jedis2.lrange("wlist", 0, -1);
            assertEquals(List.of("p", "q", "r"), restored, "warmUp should restore list in order");
        }
    }

    // -------------------------------------------------------------------------
    // Set tests
    // -------------------------------------------------------------------------

    @Test
    void testSaddAndSrem() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.sadd("tags", "java", "redis", "synclite");

            Set<String> members = jedis.smembers("tags");
            assertTrue(members.contains("java"));
            assertTrue(members.contains("redis"));
            assertTrue(members.contains("synclite"));

            // 3 rows in store
            assertEquals(3, store.select(Jedis.SETS_TABLE, Map.of("key", "tags")).size());

            jedis.srem("tags", "redis");
            assertFalse(jedis.smembers("tags").contains("redis"));
            assertEquals(2, store.select(Jedis.SETS_TABLE, Map.of("key", "tags")).size());
        }
    }

    @Test
    void testSetWarmUp() throws Exception {
        try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis1.sadd("colors", "red", "green", "blue");
        }

        try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
            raw.del("colors");
        }

        try (Jedis jedis2 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            Set<String> restored = jedis2.smembers("colors");
            assertTrue(restored.containsAll(Set.of("red", "green", "blue")), "warmUp should restore set members");
        }
    }

    // -------------------------------------------------------------------------
    // Sorted set tests
    // -------------------------------------------------------------------------

    @Test
    void testZaddAndZrem() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.zadd("scores", 10.0, "alice");
            jedis.zadd("scores", 20.0, "bob");
            jedis.zadd("scores", 15.0, "carol");

            // Redis sorted by score
            List<String> byScore = jedis.zrange("scores", 0, -1);
            assertEquals(List.of("alice", "carol", "bob"), byScore);

            // 3 rows in store
            assertEquals(3, store.select(Jedis.ZSETS_TABLE, Map.of("key", "scores")).size());

            jedis.zrem("scores", "carol");
            assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "scores")).size());
        }
    }

    @Test
    void testZaddMap() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.zadd("leaderboard", Map.of("player1", 100.0, "player2", 200.0));

            assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "leaderboard")).size());
            assertTrue(jedis.zscore("leaderboard", "player1") == 100.0);
        }
    }

    @Test
    void testZSetWarmUp() throws Exception {
        try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis1.zadd("ranking", 1.0, "gold");
            jedis1.zadd("ranking", 2.0, "silver");
            jedis1.zadd("ranking", 3.0, "bronze");
        }

        try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
            raw.del("ranking");
        }

        try (Jedis jedis2 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            List<String> restored = jedis2.zrange("ranking", 0, -1);
            assertEquals(List.of("gold", "silver", "bronze"), restored, "warmUp should restore sorted set in score order");
        }
    }

    // -------------------------------------------------------------------------
    // String extended ops
    // -------------------------------------------------------------------------

    @Test
    void testSetnx() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            long r1 = jedis.setnx("nx:key", "first");
            long r2 = jedis.setnx("nx:key", "second");

            // Redis reports 1 for new, 0 for existing
            assertEquals(1L, r1);
            assertEquals(0L, r2);

            // Store was overwritten by our setnx impl (store-first write)
            List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "nx:key"));
            assertEquals(1, rows.size());
        }
    }

    @Test
    void testMset() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.mset("m1", "v1", "m2", "v2", "m3", "v3");

            assertEquals("v1", jedis.get("m1"));
            assertEquals("v2", jedis.get("m2"));
            assertEquals("v3", jedis.get("m3"));

            assertEquals(3, store.selectAll(Jedis.STRINGS_TABLE).size());
        }
    }

    @Test
    void testGetDel() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("gd:key", "gone");

            String value = jedis.getDel("gd:key");

            assertEquals("gone", value);
            // Removed from store
            assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "gd:key")).size());
            assertNull(jedis.get("gd:key"));
        }
    }

    @Test
    void testGetSet() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("gs:key", "original");
            String old = jedis.getSet("gs:key", "updated");

            assertEquals("original", old);
            assertEquals("updated",  jedis.get("gs:key"));

            List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "gs:key"));
            assertEquals("updated", rows.get(0).get("value"));
        }
    }

    @Test
    void testSetWithSetParams() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("sp:key", "spvalue", redis.clients.jedis.params.SetParams.setParams().ex(3600));

            assertEquals("spvalue", jedis.get("sp:key"));

            List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "sp:key"));
            assertEquals(1, rows.size());
            long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
            assertTrue(expiresAt > System.currentTimeMillis(), "expires_at should be in the future for EX param");
        }
    }

    // -------------------------------------------------------------------------
    // Key expiry / lifecycle extended ops
    // -------------------------------------------------------------------------

    @Test
    void testPexpire() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("px:key", "val");
            jedis.pexpire("px:key", 3_600_000L); // 1 hour in ms

            List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "px:key"));
            long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
            assertTrue(expiresAt > System.currentTimeMillis());
        }
    }

    @Test
    void testExpireAt() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("eat:key", "val");
            long futureUnix = System.currentTimeMillis() / 1000L + 3600L;
            jedis.expireAt("eat:key", futureUnix);

            List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "eat:key"));
            long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
            assertEquals(futureUnix * 1000L, expiresAt);
        }
    }

    @Test
    void testPersist() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.setex("persist:key", 3600L, "val");

            List<Map<String, Object>> before = store.select(Jedis.STRINGS_TABLE, Map.of("key", "persist:key"));
            assertTrue(((Number) before.get(0).get("expires_at")).longValue() > 0);

            jedis.persist("persist:key");

            List<Map<String, Object>> after = store.select(Jedis.STRINGS_TABLE, Map.of("key", "persist:key"));
            assertEquals(0L, ((Number) after.get(0).get("expires_at")).longValue());
        }
    }

    @Test
    void testUnlink() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("ul:k1", "v1");
            jedis.set("ul:k2", "v2");

            jedis.unlink("ul:k1");

            assertNull(jedis.get("ul:k1"));
            assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "ul:k1")).size());
            assertEquals(1, store.select(Jedis.STRINGS_TABLE, Map.of("key", "ul:k2")).size());
        }
    }

    @Test
    void testRename() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.set("rn:old", "myvalue");

            jedis.rename("rn:old", "rn:new");

            assertEquals("myvalue", jedis.get("rn:new"));
            assertNull(jedis.get("rn:old"));

            assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "rn:old")).size());
            assertEquals(1, store.select(Jedis.STRINGS_TABLE, Map.of("key", "rn:new")).size());
        }
    }

    // -------------------------------------------------------------------------
    // List extended ops
    // -------------------------------------------------------------------------

    @Test
    void testLpushx() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            // Key does not exist — lpushx should be a no-op in the store
            jedis.lpushx("lpx:list", "nowrite");

            assertEquals(0, store.select(Jedis.LISTS_TABLE, Map.of("key", "lpx:list")).size());

            // Create the key, then lpushx should write
            jedis.rpush("lpx:list", "base");
            jedis.lpushx("lpx:list", "head");

            assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "lpx:list")).size());
            assertEquals("head", jedis.lrange("lpx:list", 0, -1).get(0));
        }
    }

    @Test
    void testRpushx() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.rpushx("rpx:list", "nowrite");
            assertEquals(0, store.select(Jedis.LISTS_TABLE, Map.of("key", "rpx:list")).size());

            jedis.rpush("rpx:list", "base");
            jedis.rpushx("rpx:list", "tail");

            assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "rpx:list")).size());
            List<String> vals = jedis.lrange("rpx:list", 0, -1);
            assertEquals("tail", vals.get(vals.size() - 1));
        }
    }

    @Test
    void testLpop() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.rpush("pop:list", "a", "b", "c");

            String head = jedis.lpop("pop:list");
            assertEquals("a", head);

            // 2 elements left in store
            assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "pop:list")).size());
        }
    }

    @Test
    void testRpop() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.rpush("rpop:list", "x", "y", "z");

            String tail = jedis.rpop("rpop:list");
            assertEquals("z", tail);

            assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "rpop:list")).size());
        }
    }

    @Test
    void testLrem() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.rpush("lrem:list", "a", "b", "a", "c", "a");

            // Remove 2 occurrences of "a" from head
            jedis.lrem("lrem:list", 2, "a");

            List<Map<String, Object>> rows = store.select(Jedis.LISTS_TABLE, Map.of("key", "lrem:list"));
            long aCount = rows.stream().filter(r -> "a".equals(r.get("value"))).count();
            assertTrue(aCount <= 1, "At most 1 'a' should remain in the store after lrem count=2");
        }
    }

    @Test
    void testLtrim() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.rpush("lt:list", "a", "b", "c", "d", "e");

            jedis.ltrim("lt:list", 1, 3);

            // Only 3 elements should remain (indices 1-3: b, c, d)
            assertEquals(3, store.select(Jedis.LISTS_TABLE, Map.of("key", "lt:list")).size());

            List<String> trimmed = jedis.lrange("lt:list", 0, -1);
            assertEquals(List.of("b", "c", "d"), trimmed);
        }
    }

    // -------------------------------------------------------------------------
    // Set extended ops
    // -------------------------------------------------------------------------

    @Test
    void testSpop() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.sadd("sp:set", "x", "y", "z");

            String popped = jedis.spop("sp:set");
            assertNotNull(popped);

            // 2 members left in store
            List<Map<String, Object>> rows = store.select(Jedis.SETS_TABLE, Map.of("key", "sp:set"));
            assertEquals(2, rows.size());
            long remaining = rows.stream().filter(r -> !popped.equals(r.get("member"))).count();
            assertEquals(2, remaining);
        }
    }

    @Test
    void testSmove() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.sadd("src:set", "apple", "banana");
            jedis.sadd("dst:set", "cherry");

            jedis.smove("src:set", "dst:set", "apple");

            // apple removed from src store
            assertEquals(0, store.select(Jedis.SETS_TABLE, Map.of("key", "src:set")).stream()
                    .filter(r -> "apple".equals(r.get("member"))).count());
            // apple added to dst store
            assertEquals(1, store.select(Jedis.SETS_TABLE, Map.of("key", "dst:set")).stream()
                    .filter(r -> "apple".equals(r.get("member"))).count());
        }
    }

    // -------------------------------------------------------------------------
    // Sorted set extended ops
    // -------------------------------------------------------------------------

    @Test
    void testZincrby() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.zadd("zi:zset", 10.0, "alice");

            double newScore = jedis.zincrby("zi:zset", 5.0, "alice");

            assertEquals(15.0, newScore, 0.001);

            List<Map<String, Object>> rows = store.select(Jedis.ZSETS_TABLE, Map.of("key", "zi:zset"));
            double stored = ((Number) rows.get(0).get("score")).doubleValue();
            assertEquals(15.0, stored, 0.001);
        }
    }

    @Test
    void testZpopmin() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.zadd("zpm:zset", 1.0, "low");
            jedis.zadd("zpm:zset", 5.0, "mid");
            jedis.zadd("zpm:zset", 9.0, "high");

            redis.clients.jedis.resps.Tuple t = jedis.zpopmin("zpm:zset");

            assertEquals("low", t.getElement());

            // 2 members remain in store
            assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "zpm:zset")).size());
        }
    }

    @Test
    void testZpopmax() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
            jedis.zadd("zpx:zset", 1.0, "low");
            jedis.zadd("zpx:zset", 5.0, "mid");
            jedis.zadd("zpx:zset", 9.0, "high");

            redis.clients.jedis.resps.Tuple t = jedis.zpopmax("zpx:zset");

            assertEquals("high", t.getElement());

            assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "zpx:zset")).size());
        }
    }

    // -------------------------------------------------------------------------
    // Store expiry purge
    // -------------------------------------------------------------------------

    /**
     * Verifies that {@link Jedis#purgeExpired()} removes rows whose TTL has
     * elapsed from every backing store table while leaving non-expired rows
     * untouched.
     *
     * <p>Strategy: write short-lived entries (TTL 1 ms, already expired by the
     * time we set {@code expires_at} to a past timestamp) alongside permanent
     * entries across all five data types, call {@code purgeExpired()} explicitly,
     * then assert only the permanent rows survive in the store.
     */
    @Test
    void testPurgeExpiredRemovesExpiredRowsFromStore() throws Exception {
        try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {

            // ── Strings ──────────────────────────────────────────────────────
            jedis.set("purge:str:keep",    "permanent");
            jedis.set("purge:str:expired", "gone");
            // Backdating expires_at to 1 ms past epoch means this row is already expired
            backdateExpiry(Jedis.STRINGS_TABLE, "key", "purge:str:expired");

            // ── Hashes ───────────────────────────────────────────────────────
            jedis.hset("purge:hash:keep",    "f1", "v1");
            jedis.hset("purge:hash:expired", "f2", "v2");
            backdateExpiry(Jedis.HASHES_TABLE, "hash_key", "purge:hash:expired");

            // ── Lists ────────────────────────────────────────────────────────
            jedis.rpush("purge:list:keep",    "item1");
            jedis.rpush("purge:list:expired", "item2");
            backdateExpiry(Jedis.LISTS_TABLE, "key", "purge:list:expired");

            // ── Sets ─────────────────────────────────────────────────────────
            jedis.sadd("purge:set:keep",    "m1");
            jedis.sadd("purge:set:expired", "m2");
            backdateExpiry(Jedis.SETS_TABLE, "key", "purge:set:expired");

            // ── Sorted sets ───────────────────────────────────────────────────
            jedis.zadd("purge:zset:keep",    1.0, "mem1");
            jedis.zadd("purge:zset:expired", 2.0, "mem2");
            backdateExpiry(Jedis.ZSETS_TABLE, "key", "purge:zset:expired");

            // ── Confirm expired rows exist before purge ───────────────────────
            assertFalse(store.select(Jedis.STRINGS_TABLE, Map.of("key",      "purge:str:expired")).isEmpty(),  "expired string row should exist before purge");
            assertFalse(store.select(Jedis.HASHES_TABLE,  Map.of("hash_key", "purge:hash:expired")).isEmpty(), "expired hash row should exist before purge");
            assertFalse(store.select(Jedis.LISTS_TABLE,   Map.of("key",      "purge:list:expired")).isEmpty(), "expired list row should exist before purge");
            assertFalse(store.select(Jedis.SETS_TABLE,    Map.of("key",      "purge:set:expired")).isEmpty(),  "expired set row should exist before purge");
            assertFalse(store.select(Jedis.ZSETS_TABLE,   Map.of("key",      "purge:zset:expired")).isEmpty(), "expired zset row should exist before purge");

            // ── Purge ─────────────────────────────────────────────────────────
            jedis.purgeExpired();

            // ── Expired rows must be gone ─────────────────────────────────────
            assertTrue(store.select(Jedis.STRINGS_TABLE, Map.of("key",      "purge:str:expired")).isEmpty(),  "expired string row should be removed by purge");
            assertTrue(store.select(Jedis.HASHES_TABLE,  Map.of("hash_key", "purge:hash:expired")).isEmpty(), "expired hash row should be removed by purge");
            assertTrue(store.select(Jedis.LISTS_TABLE,   Map.of("key",      "purge:list:expired")).isEmpty(), "expired list row should be removed by purge");
            assertTrue(store.select(Jedis.SETS_TABLE,    Map.of("key",      "purge:set:expired")).isEmpty(),  "expired set row should be removed by purge");
            assertTrue(store.select(Jedis.ZSETS_TABLE,   Map.of("key",      "purge:zset:expired")).isEmpty(), "expired zset row should be removed by purge");

            // ── Non-expired rows must survive ─────────────────────────────────
            assertFalse(store.select(Jedis.STRINGS_TABLE, Map.of("key",      "purge:str:keep")).isEmpty(),  "non-expired string row should survive purge");
            assertFalse(store.select(Jedis.HASHES_TABLE,  Map.of("hash_key", "purge:hash:keep")).isEmpty(), "non-expired hash row should survive purge");
            assertFalse(store.select(Jedis.LISTS_TABLE,   Map.of("key",      "purge:list:keep")).isEmpty(), "non-expired list row should survive purge");
            assertFalse(store.select(Jedis.SETS_TABLE,    Map.of("key",      "purge:set:keep")).isEmpty(),  "non-expired set row should survive purge");
            assertFalse(store.select(Jedis.ZSETS_TABLE,   Map.of("key",      "purge:zset:keep")).isEmpty(), "non-expired zset row should survive purge");
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Backdates the {@code expires_at} column of all rows that match
     * {@code keyCol = keyVal} in the given table to 1 ms (long in the past),
     * so that {@link Jedis#purgeExpired()} treats them as expired.
     */
    private void backdateExpiry(String table, String keyCol, String keyVal) throws Exception {
        Map<String, Object> set   = Map.of("expires_at", 1L);           // epoch + 1 ms = always expired
        Map<String, Object> where = Map.of(keyCol, keyVal);
        store.update(table, set, where);
    }

    private static void deleteRecursively(Path path) {
        if (Files.notExists(path)) return;
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) {
                    deleteRecursively(child);
                }
            } catch (IOException ignored) {}
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // On Windows the SyncLite background thread may briefly hold a
            // file lock on trace/WAL files after closeAllDevices().  Skip any
            // locked file — the next test run will clean it up.
        }
    }
}
