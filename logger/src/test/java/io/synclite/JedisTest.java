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

package io.synclite;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.fppt.jedismock.RedisServer;

/**
 * Integration test for {@link Jedis}.
 *
 * <p>Uses an in-process {@link RedisServer} (jedis-mock) so no external Redis
 * installation or Docker daemon is required.  The mock server is started once
 * for the whole test class.
 *
 * <p>All scenarios run sequentially inside a single {@code @Test} on one
 * persistent device.  The stageDir accumulates log segments across all phases
 * and is never wiped mid-test, so a consolidator pointed at the stageDir will
 * see the full, contiguous history.
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

    @Test
    void testAllJedisAPIs() throws Exception {
        Path testHome    = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("test").resolve("javalogger");
        Path testDbPath  = testHome.resolve("db").resolve("JedisTest").resolve("test.db");
        Path testStageDir = testHome.resolve("stageDir");

        // One-time cleanup from any previous run Ã¢– never repeated between phases.
        for (int attempt = 0; attempt < 20 && Files.exists(testDbPath.getParent()); attempt++) {
            try { deleteRecursively(testDbPath.getParent()); break; }
            catch (IOException e) { Thread.sleep(200); }
        }
        if (Files.exists(testStageDir)) {
            try (var dirs = Files.list(testStageDir)) {
                dirs.filter(p -> p.getFileName().toString().startsWith("synclite-jedistest-"))
                    .forEach(p -> { try { deleteRecursively(p); } catch (IOException ignored) {} });
            }
        }
        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        Path configPath = testDbPath.getParent().resolve("synclite.conf");
        Files.writeString(configPath,
                "local-data-stage-directory = " + testStageDir + "\ndevice-stage-type = FS\n");

        Class.forName("io.synclite.SQLiteStore");
        SQLiteStore.initialize(testDbPath, configPath, "jedistest");
        SyncLiteStore store = SQLiteStore.open(testDbPath);

        try {

            // –– testSetAndGet –––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("k1", "v1");
                jedis.set("k2", "v2");
                assertEquals("v1", jedis.get("k1"));
                assertEquals("v2", jedis.get("k2"));
                List<Map<String, Object>> rows = store.selectAll(Jedis.STRINGS_TABLE);
                assertTrue(rows.size() >= 2);
                List<Map<String, Object>> k1Rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "k1"));
                assertEquals(1, k1Rows.size());
                assertEquals("v1", k1Rows.get(0).get("value"));
            }

            // –– testSetOverwritesExistingKey ––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("ow:k1", "original");
                jedis.set("ow:k1", "updated");
                assertEquals("updated", jedis.get("ow:k1"));
                List<Map<String, Object>> k1Rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "ow:k1"));
                assertEquals(1, k1Rows.size());
                assertEquals("updated", k1Rows.get(0).get("value"));
            }

            // –– testDel –––––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("del:k1", "v1");
                jedis.set("del:k2", "v2");
                jedis.del("del:k1");
                assertNull(jedis.get("del:k1"), "Deleted key should return null from Redis");
                assertEquals("v2", jedis.get("del:k2"));
                assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "del:k1")).size());
                assertEquals(1, store.select(Jedis.STRINGS_TABLE, Map.of("key", "del:k2")).size());
            }

            // –– testMultiKeyDel –––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("md:k1", "v1");
                jedis.set("md:k2", "v2");
                jedis.set("md:k3", "v3");
                jedis.del("md:k1", "md:k3");
                assertNull(jedis.get("md:k1"));
                assertNull(jedis.get("md:k3"));
                assertEquals("v2", jedis.get("md:k2"));
                assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "md:k1")).size());
                assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "md:k3")).size());
            }

            // –– testHsetAndHget ––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.hset("user:1", "name", "Alice");
                jedis.hset("user:1", "email", "alice@example.com");
                assertEquals("Alice",             jedis.hget("user:1", "name"));
                assertEquals("alice@example.com", jedis.hget("user:1", "email"));
                List<Map<String, Object>> hashRows = store.select(Jedis.HASHES_TABLE, Map.of("hash_key", "user:1"));
                assertEquals(2, hashRows.size());
            }

            // –– testHsetMap ––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.hset("session:42", Map.of("token", "abc123", "user", "bob"));
                assertEquals("abc123", jedis.hget("session:42", "token"));
                assertEquals("bob",    jedis.hget("session:42", "user"));
                assertEquals(2, store.select(Jedis.HASHES_TABLE, Map.of("hash_key", "session:42")).size());
            }

            // –– testHdel ––––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.hset("hdel:user", "name", "Alice");
                jedis.hset("hdel:user", "age", "30");
                jedis.hdel("hdel:user", "age");
                assertNull(jedis.hget("hdel:user", "age"), "Deleted hash field should be null");
                assertEquals("Alice", jedis.hget("hdel:user", "name"), "Other fields unaffected");
                List<Map<String, Object>> remaining = store.select(Jedis.HASHES_TABLE, Map.of("hash_key", "hdel:user"));
                assertEquals(1, remaining.size());
                assertEquals("name", remaining.get(0).get("field"));
            }

            // –– testDelRemovesHashEntries ––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.hset("drh:user", "name", "Alice");
                jedis.hset("drh:user", "role", "admin");
                jedis.del("drh:user");
                assertEquals(0, store.select(Jedis.HASHES_TABLE, Map.of("hash_key", "drh:user")).size());
            }

            // –– testWarmUpRebuildsCacheFromStore ––––––––––––––––––––––––––––––
            try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis1.set("rebuild:str", "hello");
                jedis1.hset("rebuild:hash", "field1", "world");
            }
            try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
                raw.del("rebuild:str");
                raw.del("rebuild:hash");
                assertNull(raw.get("rebuild:str"),             "Redis should be empty after manual del");
                assertNull(raw.hget("rebuild:hash", "field1"), "Redis should be empty after manual del");
            }
            try (Jedis jedis2 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                assertEquals("hello", jedis2.get("rebuild:str"),             "warmUp should restore string key");
                assertEquals("world", jedis2.hget("rebuild:hash", "field1"), "warmUp should restore hash field");
            }

            // –– testRestartReloadsAllTypesFromStore –––––––––––––––––––––––––––
            // Phase R1: write data
            try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis1.set("rs:str",  "hello");
                jedis1.setex("rs:str:ttl", 3600L, "withttl");
                jedis1.hset("rs:hash", Map.of("field1", "v1", "field2", "v2"));
                jedis1.rpush("rs:list", "a", "b", "c");
                jedis1.sadd("rs:set", "x", "y", "z");
                jedis1.zadd("rs:zset", Map.of("gold", 1.0, "silver", 2.0, "bronze", 3.0));
            }
            // Phase R2: application shutdown
            store.close();
            store = null;
            SQLiteStore.closeAllDevices();
            Thread.sleep(200);
            // Phase R3: Redis restart (full cache wipe)
            try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
                raw.flushAll();
                assertNull(raw.get("rs:str"),                       "cache should be empty after flushAll");
                assertTrue(raw.hgetAll("rs:hash").isEmpty(),        "cache should be empty after flushAll");
                assertTrue(raw.lrange("rs:list", 0, -1).isEmpty(), "cache should be empty after flushAll");
                assertTrue(raw.smembers("rs:set").isEmpty(),       "cache should be empty after flushAll");
                assertTrue(raw.zrange("rs:zset", 0, -1).isEmpty(), "cache should be empty after flushAll");
            }
            // Phase R4: application restart Ã¢– re-open existing store, no initialize()
            store = SQLiteStore.open(testDbPath);
            try (Jedis jedis2 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                assertEquals("hello",   jedis2.get("rs:str"),            "String key restored after restart");
                assertEquals("withttl", jedis2.get("rs:str:ttl"),        "TTL string key restored after restart");
                assertEquals("v1",      jedis2.hget("rs:hash", "field1"), "Hash field1 restored");
                assertEquals("v2",      jedis2.hget("rs:hash", "field2"), "Hash field2 restored");
                assertEquals(List.of("a", "b", "c"), jedis2.lrange("rs:list", 0, -1), "List restored in insertion order");
                Set<String> restoredSet = jedis2.smembers("rs:set");
                assertTrue(restoredSet.containsAll(Set.of("x", "y", "z")), "All set members restored");
                assertEquals(List.of("gold", "silver", "bronze"), jedis2.zrange("rs:zset", 0, -1), "ZSet restored in score order");
            }

            // –– testSetex –––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.setex("ttl:key", 3600L, "ttlvalue");
                assertEquals("ttlvalue", jedis.get("ttl:key"));
                List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "ttl:key"));
                assertEquals(1, rows.size());
                long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
                assertTrue(expiresAt > System.currentTimeMillis(), "expires_at should be in the future");
            }

            // –– testRpushAndListStore –––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.rpush("mylist", "a", "b", "c");
                List<String> redisValues = jedis.lrange("mylist", 0, -1);
                assertEquals(List.of("a", "b", "c"), redisValues);
                List<Map<String, Object>> rows = store.select(Jedis.LISTS_TABLE, Map.of("key", "mylist"));
                assertEquals(3, rows.size());
            }

            // –– testLpush –––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.rpush("llist", "x");
                jedis.lpush("llist", "y");
                List<String> redisValues = jedis.lrange("llist", 0, -1);
                assertEquals("y", redisValues.get(0), "lpush value should be at the head");
                assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "llist")).size());
            }

            // –– testListWarmUp ––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis1 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis1.rpush("wlist", "p", "q", "r");
            }
            try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
                raw.del("wlist");
            }
            try (Jedis jedis2 = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                List<String> restored = jedis2.lrange("wlist", 0, -1);
                assertEquals(List.of("p", "q", "r"), restored, "warmUp should restore list in order");
            }

            // –– testSaddAndSrem –––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.sadd("tags", "java", "redis", "synclite");
                Set<String> members = jedis.smembers("tags");
                assertTrue(members.contains("java"));
                assertTrue(members.contains("redis"));
                assertTrue(members.contains("synclite"));
                assertEquals(3, store.select(Jedis.SETS_TABLE, Map.of("key", "tags")).size());
                jedis.srem("tags", "redis");
                assertFalse(jedis.smembers("tags").contains("redis"));
                assertEquals(2, store.select(Jedis.SETS_TABLE, Map.of("key", "tags")).size());
            }

            // –– testSetWarmUp –––––––––––––––––––––––––––––––––––––––––––––––––
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

            // –– testZaddAndZrem –––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.zadd("scores", 10.0, "alice");
                jedis.zadd("scores", 20.0, "bob");
                jedis.zadd("scores", 15.0, "carol");
                List<String> byScore = jedis.zrange("scores", 0, -1);
                assertEquals(List.of("alice", "carol", "bob"), byScore);
                assertEquals(3, store.select(Jedis.ZSETS_TABLE, Map.of("key", "scores")).size());
                jedis.zrem("scores", "carol");
                assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "scores")).size());
            }

            // –– testZaddMap –––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.zadd("leaderboard", Map.of("player1", 100.0, "player2", 200.0));
                assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "leaderboard")).size());
                assertTrue(jedis.zscore("leaderboard", "player1") == 100.0);
            }

            // –– testZSetWarmUp ––––––––––––––––––––––––––––––––––––––––––––––––
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

            // –– testSetnx –––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                long r1 = jedis.setnx("nx:key", "first");
                long r2 = jedis.setnx("nx:key", "second");
                assertEquals(1L, r1);
                assertEquals(0L, r2);
                List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "nx:key"));
                assertEquals(1, rows.size());
            }

            // –– testMset ––––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.mset("m1", "v1", "m2", "v2", "m3", "v3");
                assertEquals("v1", jedis.get("m1"));
                assertEquals("v2", jedis.get("m2"));
                assertEquals("v3", jedis.get("m3"));
                assertEquals(3, store.select(Jedis.STRINGS_TABLE, Map.of("key", "m1")).size() +
                                 store.select(Jedis.STRINGS_TABLE, Map.of("key", "m2")).size() +
                                 store.select(Jedis.STRINGS_TABLE, Map.of("key", "m3")).size());
            }

            // –– testGetDel ––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("gd:key", "gone");
                String value = jedis.getDel("gd:key");
                assertEquals("gone", value);
                assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "gd:key")).size());
                assertNull(jedis.get("gd:key"));
            }

            // –– testGetSet ––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("gs:key", "original");
                String old = jedis.getSet("gs:key", "updated");
                assertEquals("original", old);
                assertEquals("updated",  jedis.get("gs:key"));
                List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "gs:key"));
                assertEquals("updated", rows.get(0).get("value"));
            }

            // –– testSetWithSetParams ––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("sp:key", "spvalue", redis.clients.jedis.params.SetParams.setParams().ex(3600));
                assertEquals("spvalue", jedis.get("sp:key"));
                List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "sp:key"));
                assertEquals(1, rows.size());
                long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
                assertTrue(expiresAt > System.currentTimeMillis(), "expires_at should be in the future for EX param");
            }

            // –– testPexpire –––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("px:key", "val");
                jedis.pexpire("px:key", 3_600_000L);
                List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "px:key"));
                long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
                assertTrue(expiresAt > System.currentTimeMillis());
            }

            // –– testExpireAt ––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("eat:key", "val");
                long futureUnix = System.currentTimeMillis() / 1000L + 3600L;
                jedis.expireAt("eat:key", futureUnix);
                List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "eat:key"));
                long expiresAt = ((Number) rows.get(0).get("expires_at")).longValue();
                assertEquals(futureUnix * 1000L, expiresAt);
            }

            // –– testPersist –––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.setex("persist:key", 3600L, "val");
                List<Map<String, Object>> before = store.select(Jedis.STRINGS_TABLE, Map.of("key", "persist:key"));
                assertTrue(((Number) before.get(0).get("expires_at")).longValue() > 0);
                jedis.persist("persist:key");
                List<Map<String, Object>> after = store.select(Jedis.STRINGS_TABLE, Map.of("key", "persist:key"));
                assertEquals(0L, ((Number) after.get(0).get("expires_at")).longValue());
            }

            // –– testUnlink ––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("ul:k1", "v1");
                jedis.set("ul:k2", "v2");
                jedis.unlink("ul:k1");
                assertNull(jedis.get("ul:k1"));
                assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "ul:k1")).size());
                assertEquals(1, store.select(Jedis.STRINGS_TABLE, Map.of("key", "ul:k2")).size());
            }

            // –– testRename ––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.set("rn:old", "myvalue");
                jedis.rename("rn:old", "rn:new");
                assertEquals("myvalue", jedis.get("rn:new"));
                assertNull(jedis.get("rn:old"));
                assertEquals(0, store.select(Jedis.STRINGS_TABLE, Map.of("key", "rn:old")).size());
                assertEquals(1, store.select(Jedis.STRINGS_TABLE, Map.of("key", "rn:new")).size());
            }

            // –– testLpushx ––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.lpushx("lpx:list", "nowrite");
                assertEquals(0, store.select(Jedis.LISTS_TABLE, Map.of("key", "lpx:list")).size());
                jedis.rpush("lpx:list", "base");
                jedis.lpushx("lpx:list", "head");
                assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "lpx:list")).size());
                assertEquals("head", jedis.lrange("lpx:list", 0, -1).get(0));
            }

            // –– testRpushx ––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.rpushx("rpx:list", "nowrite");
                assertEquals(0, store.select(Jedis.LISTS_TABLE, Map.of("key", "rpx:list")).size());
                jedis.rpush("rpx:list", "base");
                jedis.rpushx("rpx:list", "tail");
                assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "rpx:list")).size());
                List<String> vals = jedis.lrange("rpx:list", 0, -1);
                assertEquals("tail", vals.get(vals.size() - 1));
            }

            // –– testLpop ––––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.rpush("pop:list", "a", "b", "c");
                String head = jedis.lpop("pop:list");
                assertEquals("a", head);
                assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "pop:list")).size());
            }

            // –– testRpop ––––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.rpush("rpop:list", "x", "y", "z");
                String tail = jedis.rpop("rpop:list");
                assertEquals("z", tail);
                assertEquals(2, store.select(Jedis.LISTS_TABLE, Map.of("key", "rpop:list")).size());
            }

            // –– testLrem ––––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.rpush("lrem:list", "a", "b", "a", "c", "a");
                jedis.lrem("lrem:list", 2, "a");
                List<Map<String, Object>> rows = store.select(Jedis.LISTS_TABLE, Map.of("key", "lrem:list"));
                long aCount = rows.stream().filter(r -> "a".equals(r.get("value"))).count();
                assertTrue(aCount <= 1, "At most 1 'a' should remain in the store after lrem count=2");
            }

            // –– testLtrim –––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.rpush("lt:list", "a", "b", "c", "d", "e");
                jedis.ltrim("lt:list", 1, 3);
                assertEquals(3, store.select(Jedis.LISTS_TABLE, Map.of("key", "lt:list")).size());
                List<String> trimmed = jedis.lrange("lt:list", 0, -1);
                assertEquals(List.of("b", "c", "d"), trimmed);
            }

            // –– testSpop ––––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.sadd("sp:set", "x", "y", "z");
                String popped = jedis.spop("sp:set");
                assertNotNull(popped);
                List<Map<String, Object>> rows = store.select(Jedis.SETS_TABLE, Map.of("key", "sp:set"));
                assertEquals(2, rows.size());
                long remaining = rows.stream().filter(r -> !popped.equals(r.get("member"))).count();
                assertEquals(2, remaining);
            }

            // –– testSmove –––––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.sadd("src:set", "apple", "banana");
                jedis.sadd("dst:set", "cherry");
                jedis.smove("src:set", "dst:set", "apple");
                assertEquals(0, store.select(Jedis.SETS_TABLE, Map.of("key", "src:set")).stream()
                        .filter(r -> "apple".equals(r.get("member"))).count());
                assertEquals(1, store.select(Jedis.SETS_TABLE, Map.of("key", "dst:set")).stream()
                        .filter(r -> "apple".equals(r.get("member"))).count());
            }

            // –– testZincrby –––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.zadd("zi:zset", 10.0, "alice");
                double newScore = jedis.zincrby("zi:zset", 5.0, "alice");
                assertEquals(15.0, newScore, 0.001);
                List<Map<String, Object>> rows = store.select(Jedis.ZSETS_TABLE, Map.of("key", "zi:zset"));
                double stored = ((Number) rows.get(0).get("score")).doubleValue();
                assertEquals(15.0, stored, 0.001);
            }

            // –– testZpopmin –––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.zadd("zpm:zset", 1.0, "low");
                jedis.zadd("zpm:zset", 5.0, "mid");
                jedis.zadd("zpm:zset", 9.0, "high");
                redis.clients.jedis.resps.Tuple t = jedis.zpopmin("zpm:zset");
                assertEquals("low", t.getElement());
                assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "zpm:zset")).size());
            }

            // –– testZpopmax –––––––––––––––––––––––––––––––––––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                jedis.zadd("zpx:zset", 1.0, "low");
                jedis.zadd("zpx:zset", 5.0, "mid");
                jedis.zadd("zpx:zset", 9.0, "high");
                redis.clients.jedis.resps.Tuple t = jedis.zpopmax("zpx:zset");
                assertEquals("high", t.getElement());
                assertEquals(2, store.select(Jedis.ZSETS_TABLE, Map.of("key", "zpx:zset")).size());
            }

            // –– testPurgeExpiredRemovesExpiredRowsFromStore –––––––––––––––––––
            try (Jedis jedis = Jedis.builder(store).host(redisHost).port(redisPort).build()) {
                // Strings
                jedis.set("purge:str:keep",    "permanent");
                jedis.set("purge:str:expired", "gone");
                backdateExpiry(store, Jedis.STRINGS_TABLE, "key", "purge:str:expired");
                // Hashes
                jedis.hset("purge:hash:keep",    "f1", "v1");
                jedis.hset("purge:hash:expired", "f2", "v2");
                backdateExpiry(store, Jedis.HASHES_TABLE, "hash_key", "purge:hash:expired");
                // Lists
                jedis.rpush("purge:list:keep",    "item1");
                jedis.rpush("purge:list:expired", "item2");
                backdateExpiry(store, Jedis.LISTS_TABLE, "key", "purge:list:expired");
                // Sets
                jedis.sadd("purge:set:keep",    "m1");
                jedis.sadd("purge:set:expired", "m2");
                backdateExpiry(store, Jedis.SETS_TABLE, "key", "purge:set:expired");
                // Sorted sets
                jedis.zadd("purge:zset:keep",    1.0, "mem1");
                jedis.zadd("purge:zset:expired", 2.0, "mem2");
                backdateExpiry(store, Jedis.ZSETS_TABLE, "key", "purge:zset:expired");

                // Confirm expired rows exist before purge
                assertFalse(store.select(Jedis.STRINGS_TABLE, Map.of("key",      "purge:str:expired")).isEmpty(),  "expired string row should exist before purge");
                assertFalse(store.select(Jedis.HASHES_TABLE,  Map.of("hash_key", "purge:hash:expired")).isEmpty(), "expired hash row should exist before purge");
                assertFalse(store.select(Jedis.LISTS_TABLE,   Map.of("key",      "purge:list:expired")).isEmpty(), "expired list row should exist before purge");
                assertFalse(store.select(Jedis.SETS_TABLE,    Map.of("key",      "purge:set:expired")).isEmpty(),  "expired set row should exist before purge");
                assertFalse(store.select(Jedis.ZSETS_TABLE,   Map.of("key",      "purge:zset:expired")).isEmpty(), "expired zset row should exist before purge");

                jedis.purgeExpired();

                // Expired rows must be gone
                assertTrue(store.select(Jedis.STRINGS_TABLE, Map.of("key",      "purge:str:expired")).isEmpty(),  "expired string row should be removed by purge");
                assertTrue(store.select(Jedis.HASHES_TABLE,  Map.of("hash_key", "purge:hash:expired")).isEmpty(), "expired hash row should be removed by purge");
                assertTrue(store.select(Jedis.LISTS_TABLE,   Map.of("key",      "purge:list:expired")).isEmpty(), "expired list row should be removed by purge");
                assertTrue(store.select(Jedis.SETS_TABLE,    Map.of("key",      "purge:set:expired")).isEmpty(),  "expired set row should be removed by purge");
                assertTrue(store.select(Jedis.ZSETS_TABLE,   Map.of("key",      "purge:zset:expired")).isEmpty(), "expired zset row should be removed by purge");

                // Non-expired rows must survive
                assertFalse(store.select(Jedis.STRINGS_TABLE, Map.of("key",      "purge:str:keep")).isEmpty(),  "non-expired string row should survive purge");
                assertFalse(store.select(Jedis.HASHES_TABLE,  Map.of("hash_key", "purge:hash:keep")).isEmpty(), "non-expired hash row should survive purge");
                assertFalse(store.select(Jedis.LISTS_TABLE,   Map.of("key",      "purge:list:keep")).isEmpty(), "non-expired list row should survive purge");
                assertFalse(store.select(Jedis.SETS_TABLE,    Map.of("key",      "purge:set:keep")).isEmpty(),  "non-expired set row should survive purge");
                assertFalse(store.select(Jedis.ZSETS_TABLE,   Map.of("key",      "purge:zset:keep")).isEmpty(), "non-expired zset row should survive purge");
            }

        } finally {
            if (store != null) {
                try { store.close(); } catch (Exception ignored) {}
            }
            try { SQLiteStore.closeAllDevices(); } catch (Exception ignored) {}
            Thread.sleep(150);
        }
    }

    @Test
    void testManagedBuilderAutoInitializesStoreLifecycle() throws Exception {
        Path testHome = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("test").resolve("javalogger");
        Path testDbPath = testHome.resolve("db").resolve("JedisManagedBuilderTest").resolve("test.db");
        Path testStageDir = testHome.resolve("stageDir");

        for (int attempt = 0; attempt < 20 && Files.exists(testDbPath.getParent()); attempt++) {
            try { deleteRecursively(testDbPath.getParent()); break; }
            catch (IOException e) { Thread.sleep(200); }
        }
        if (Files.exists(testStageDir)) {
            try (var dirs = Files.list(testStageDir)) {
                dirs.filter(p -> p.getFileName().toString().startsWith("synclite-jedismanaged-"))
                    .forEach(p -> { try { deleteRecursively(p); } catch (IOException ignored) {} });
            }
        }

        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        Path configPath = testDbPath.getParent().resolve("synclite.conf");
        Files.writeString(configPath,
                "local-data-stage-directory = " + testStageDir + "\n" +
                "device-stage-type = FS\n");

        try (Jedis jedis = Jedis.builder(testDbPath, configPath, "jedismanaged")
                .host(redisHost)
                .port(redisPort)
                .build()) {
            jedis.set("managed:k1", "v1");
            assertEquals("v1", jedis.get("managed:k1"));
        }

        // Store/device lifecycle is managed by Jedis; data should still be durable.
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            List<Map<String, Object>> rows = store.select(Jedis.STRINGS_TABLE, Map.of("key", "managed:k1"));
            assertEquals(1, rows.size());
            assertEquals("v1", rows.get(0).get("value"));
        }

        // Validate warm-up from store using managed builder only (no explicit initialize/open).
        try (redis.clients.jedis.Jedis raw = new redis.clients.jedis.Jedis(redisHost, redisPort)) {
            raw.del("managed:k1");
            assertNull(raw.get("managed:k1"));
        }

        try (Jedis jedis = Jedis.builder(testDbPath, configPath, "jedismanaged")
                .host(redisHost)
                .port(redisPort)
                .build()) {
            assertEquals("v1", jedis.get("managed:k1"));
        }

        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Backdates the {@code expires_at} column of all rows that match
     * {@code keyCol = keyVal} in the given table to 1 ms (long in the past),
     * so that {@link Jedis#purgeExpired()} treats them as expired.
     */
    private static void backdateExpiry(SyncLiteStore store, String table, String keyCol, String keyVal) throws Exception {
        Map<String, Object> set   = Map.of("expires_at", 1L);
        Map<String, Object> where = Map.of(keyCol, keyVal);
        store.update(table, set, where);
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (Files.notExists(path)) return;
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
