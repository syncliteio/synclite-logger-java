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
 *
 */

package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.args.BitOP;
import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.args.ListPosition;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.GeoAddParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.SortingParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

/**
 * A SyncLite-backed drop-in replacement for {@link redis.clients.jedis.Jedis}.
 *
 * <p>Every write (set, hset, del, expire …) is first durably committed to a
 * {@link SyncLiteStore} — which captures it to the SyncLite replication log for
 * downstream CDC consumption — and then forwarded to Redis.  On startup the cache
 * is automatically re-populated from the store so that Redis data survives
 * restarts.
 *
 * <p><strong>Usage:</strong>
 * <pre>
 *   // Explicit store lifecycle (advanced)
 *   SQLiteStore.initialize(dbPath, configPath);
 *   try (SyncLiteStore store = SQLiteStore.open(dbPath);
 *        Jedis jedis = Jedis.builder(store).host("redis-host").port(6380).build()) {
 *
 *       jedis.set("user:1:name", "Alice");
 *       jedis.hset("session:42", "token", "abc123");
 *       String name = jedis.get("user:1:name");   // reads from Redis
 *   }
 *
 *   // Managed store lifecycle (simple)
 *   try (Jedis jedis = Jedis.builder(dbPath, configPath, "jedis-device")
 *           .host("redis-host").port(6380).build()) {
 *       jedis.set("user:1:name", "Alice");
 *   }
 * </pre>
 *
 * <p><strong>Store schema:</strong>
 * <ul>
 *   <li>{@code jedis_strings (key TEXT, value TEXT, expires_at BIGINT)} — string keys</li>
 *   <li>{@code jedis_hashes  (hash_key TEXT, field TEXT, value TEXT, expires_at BIGINT)} — hash keys</li>
 *   <li>{@code jedis_lists   (key TEXT, idx BIGINT, value TEXT, expires_at BIGINT)} — list keys</li>
 *   <li>{@code jedis_sets    (key TEXT, member TEXT, expires_at BIGINT)} — set keys</li>
 *   <li>{@code jedis_zsets   (key TEXT, score DOUBLE, member TEXT, expires_at BIGINT)} — sorted set keys</li>
 * </ul>
 *
 * <p><strong>Thread safety:</strong> A single {@code Jedis} instance (like the
 * upstream Jedis client) is <em>not</em> thread-safe.  Use one instance per thread,
 * each backed by its own {@link SyncLiteStore} connection.
 *
 * <p><strong>Expired-entry purge:</strong> A background daemon thread runs
 * {@link #purgeExpired()} at a configurable interval (default 60 s) to remove
 * TTL-expired rows from the store.  Configure with {@link Builder#storePurgeInterval(long, TimeUnit)}.
 * The scheduler is shut down automatically when {@link #close()} is called.
 */
public class Jedis extends redis.clients.jedis.Jedis {

    static final String STRINGS_TABLE = "jedis_strings";
    static final String HASHES_TABLE   = "jedis_hashes";
    static final String LISTS_TABLE    = "jedis_lists";
    static final String SETS_TABLE     = "jedis_sets";
    static final String ZSETS_TABLE    = "jedis_zsets";

    private static final long DEFAULT_PURGE_INTERVAL_SECONDS = 60L;

    private final SyncLiteStore store;
    private final boolean managesStoreLifecycle;
    private final Path managedStoreDbPath;
    private final ScheduledExecutorService purgeScheduler;

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    /**
     * Returns a new {@link Builder} pre-set with the given store.
     *
     * @param store a pre-opened {@link SyncLiteStore} — not closed by the built instance
     */
    public static Builder builder(SyncLiteStore store) {
        return new Builder().store(store);
    }

    /**
     * Returns a new {@link Builder} that manages a SQLiteStore lifecycle internally.
     *
     * <p>On {@link Builder#build()}, this path will call:
     * <pre>
     *   SQLiteStore.initialize(storeDbPath, configPath)
     *   SQLiteStore.open(storeDbPath)
     * </pre>
     * and on {@link #close()} it will close the opened store and device.
     */
    public static Builder builder(Path storeDbPath, Path configPath) {
        return new Builder().sqliteStore(storeDbPath, configPath);
    }

    /**
     * Same as {@link #builder(Path, Path)} with explicit SyncLite device name.
     */
    public static Builder builder(Path storeDbPath, Path configPath, String deviceName) {
        return new Builder().sqliteStore(storeDbPath, configPath, deviceName);
    }

    /**
     * Returns a new {@link Builder} pre-set with the Redis host and port.
     * Call {@link Builder#store(SyncLiteStore)} before {@link Builder#build()}.
     */
    public static Builder builder(String host, int port) {
        return new Builder().host(host).port(port);
    }

    /**
     * Fluent builder for {@link Jedis}.
     *
     * <pre>
     *   // store-first
     *   Jedis jedis = Jedis.builder(store).host("redis-host").port(6380).build();
     *
     *   // host/port-first
     *   Jedis jedis = Jedis.builder("redis-host", 6380).store(store).build();
     * </pre>
     */
    public static final class Builder {
        private SyncLiteStore store;
        private String   host          = "localhost";
        private int      port          = 6379;
        private long     purgeInterval = DEFAULT_PURGE_INTERVAL_SECONDS;
        private TimeUnit purgeUnit     = TimeUnit.SECONDS;
        private boolean  manageStoreLifecycle;
        private Path     managedStoreDbPath;
        private Path     managedStoreConfigPath;
        private String   managedStoreDeviceName;

        private Builder() {}

        public Builder store(SyncLiteStore store) {
            this.store = store;
            this.manageStoreLifecycle = false;
            this.managedStoreDbPath = null;
            this.managedStoreConfigPath = null;
            this.managedStoreDeviceName = null;
            return this;
        }

        /**
         * Configures this builder to initialize/open SQLiteStore internally.
         */
        public Builder sqliteStore(Path dbPath, Path configPath) {
            this.store = null;
            this.manageStoreLifecycle = true;
            this.managedStoreDbPath = dbPath;
            this.managedStoreConfigPath = configPath;
            this.managedStoreDeviceName = null;
            return this;
        }

        /**
         * Configures this builder to initialize/open SQLiteStore internally
         * with a custom SyncLite device name.
         */
        public Builder sqliteStore(Path dbPath, Path configPath, String deviceName) {
            this.store = null;
            this.manageStoreLifecycle = true;
            this.managedStoreDbPath = dbPath;
            this.managedStoreConfigPath = configPath;
            this.managedStoreDeviceName = deviceName;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /** Overrides the default 60-second background purge interval. */
        public Builder storePurgeInterval(long interval, TimeUnit unit) {
            this.purgeInterval = interval;
            this.purgeUnit     = unit;
            return this;
        }

        public Jedis build() throws SQLException {
            SyncLiteStore resolvedStore = this.store;
            boolean resolvedManaged = false;
            Path resolvedDbPath = null;

            if (resolvedStore == null) {
                if (!manageStoreLifecycle || managedStoreDbPath == null || managedStoreConfigPath == null) {
                    throw new IllegalStateException("Either provide store(...) or sqliteStore(dbPath, configPath)");
                }

                if (managedStoreDeviceName != null && !managedStoreDeviceName.isBlank()) {
                    SQLiteStore.initialize(managedStoreDbPath, managedStoreConfigPath, managedStoreDeviceName);
                } else {
                    SQLiteStore.initialize(managedStoreDbPath, managedStoreConfigPath);
                }
                resolvedStore = SQLiteStore.open(managedStoreDbPath);
                resolvedManaged = true;
                resolvedDbPath = managedStoreDbPath;
            }

            this.store = resolvedStore;

            try {
                return new Jedis(this, resolvedManaged, resolvedDbPath);
            } catch (SQLException e) {
                if (resolvedManaged && resolvedStore != null) {
                    try {
                        resolvedStore.close();
                    } catch (SQLException ignored) {
                    }
                    if (resolvedDbPath != null) {
                        try {
                            SQLiteStore.closeDevice(resolvedDbPath);
                        } catch (SQLException ignored) {
                        }
                    }
                }
                throw e;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Constructor (private — use Builder)
    // -------------------------------------------------------------------------

    private Jedis(Builder b, boolean managesStoreLifecycle, Path managedStoreDbPath) throws SQLException {
        super(b.host, b.port);
        this.store = b.store;
        this.managesStoreLifecycle = managesStoreLifecycle;
        this.managedStoreDbPath = managedStoreDbPath;
        this.purgeScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jedis-purge");
            t.setDaemon(true);
            return t;
        });
        initStoreTables();
        warmUp();
        purgeScheduler.scheduleAtFixedRate(() -> {
            try {
                purgeExpired();
            } catch (Exception e) {
                // do not crash the scheduler thread
            }
        }, b.purgeInterval, b.purgeInterval, b.purgeUnit);
    }

    // -------------------------------------------------------------------------
    // Initialisation
    // -------------------------------------------------------------------------

    private void initStoreTables() throws SQLException {
        String st = store.getDefaultStringType();

        Map<String, String> stringsCols = new LinkedHashMap<>();
        stringsCols.put("key",        st + " PRIMARY KEY");
        stringsCols.put("value",      st);
        stringsCols.put("expires_at", "BIGINT");
        store.createTable(STRINGS_TABLE, stringsCols);

        Map<String, String> hashesCols = new LinkedHashMap<>();
        hashesCols.put("hash_key",   st);
        hashesCols.put("field",      st);
        hashesCols.put("value",      st);
        hashesCols.put("expires_at", "BIGINT");
        hashesCols.put("PRIMARY KEY", "(hash_key, field)");
        store.createTable(HASHES_TABLE, hashesCols);

        Map<String, String> listsCols = new LinkedHashMap<>();
        listsCols.put("key",        st);
        listsCols.put("idx",        "BIGINT");
        listsCols.put("value",      st);
        listsCols.put("expires_at", "BIGINT");
        listsCols.put("PRIMARY KEY", "(key, idx)");
        store.createTable(LISTS_TABLE, listsCols);

        Map<String, String> setsCols = new LinkedHashMap<>();
        setsCols.put("key",        st);
        setsCols.put("member",     st);
        setsCols.put("expires_at", "BIGINT");
        setsCols.put("PRIMARY KEY", "(key, member)");
        store.createTable(SETS_TABLE, setsCols);

        Map<String, String> zsetsCols = new LinkedHashMap<>();
        zsetsCols.put("key",        st);
        zsetsCols.put("score",      "DOUBLE");
        zsetsCols.put("member",     st);
        zsetsCols.put("expires_at", "BIGINT");
        zsetsCols.put("PRIMARY KEY", "(key, member)");
        store.createTable(ZSETS_TABLE, zsetsCols);
    }

    /**
     * Loads all non-expired entries from the backing store into Redis.
     * Called automatically in the constructor; callers may also invoke it explicitly
     * to re-synchronise Redis from the store (e.g. after a targeted Redis flush).
     */
    public void warmUp() throws SQLException {
        long now = System.currentTimeMillis();

        // --- strings ---
        List<Map<String, Object>> stringRows = store.selectAll(STRINGS_TABLE);
        for (Map<String, Object> row : stringRows) {
            String key   = (String) row.get("key");
            String value = (String) row.get("value");
            long expiresAt = toLong(row.get("expires_at"));

            if (expiresAt > 0 && expiresAt <= now) {
                continue; // expired — skip (cleaned up lazily; see expire())
            }
            if (expiresAt > 0) {
                super.psetex(key, expiresAt - now, value);
            } else {
                super.set(key, value);
            }
        }

        // --- hashes ---
        // Collect field-value pairs per hash key, honouring expiry.
        Map<String, Map<String, String>> hashes = new LinkedHashMap<>();
        Map<String, Long> hashExpiry = new HashMap<>();

        List<Map<String, Object>> hashRows = store.selectAll(HASHES_TABLE);
        for (Map<String, Object> row : hashRows) {
            String hashKey = (String) row.get("hash_key");
            String field   = (String) row.get("field");
            String value   = (String) row.get("value");
            long expiresAt = toLong(row.get("expires_at"));

            if (expiresAt > 0 && expiresAt <= now) {
                continue; // expired
            }
            hashes.computeIfAbsent(hashKey, k -> new LinkedHashMap<>()).put(field, value);
            if (expiresAt > 0) {
                hashExpiry.put(hashKey, expiresAt);
            }
        }

        for (Map.Entry<String, Map<String, String>> entry : hashes.entrySet()) {
            String hashKey = entry.getKey();
            super.hset(hashKey, entry.getValue());
            Long exp = hashExpiry.get(hashKey);
            if (exp != null) {
                super.expire(hashKey, Math.max(1L, (exp - now) / 1000L));
            }
        }

        // --- lists ---
        // Collect ordered values per key (sorted by idx), honouring expiry.
        Map<String, List<String>> lists = new LinkedHashMap<>();
        Map<String, Long> listExpiry = new HashMap<>();

        List<Map<String, Object>> listRows = store.selectAll(LISTS_TABLE);
        // Sort by idx within each key to preserve order.
        listRows.sort((a, b) -> Long.compare(toLong(a.get("idx")), toLong(b.get("idx"))));
        for (Map<String, Object> row : listRows) {
            String key     = (String) row.get("key");
            String value   = (String) row.get("value");
            long expiresAt = toLong(row.get("expires_at"));
            if (expiresAt > 0 && expiresAt <= now) continue;
            lists.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
            if (expiresAt > 0) listExpiry.put(key, expiresAt);
        }
        for (Map.Entry<String, List<String>> entry : lists.entrySet()) {
            String key = entry.getKey();
            String[] vals = entry.getValue().toArray(new String[0]);
            super.rpush(key, vals);
            Long exp = listExpiry.get(key);
            if (exp != null) super.expire(key, Math.max(1L, (exp - now) / 1000L));
        }

        // --- sets ---
        Map<String, List<String>> sets = new LinkedHashMap<>();
        Map<String, Long> setExpiry = new HashMap<>();

        List<Map<String, Object>> setRows = store.selectAll(SETS_TABLE);
        for (Map<String, Object> row : setRows) {
            String key     = (String) row.get("key");
            String member  = (String) row.get("member");
            long expiresAt = toLong(row.get("expires_at"));
            if (expiresAt > 0 && expiresAt <= now) continue;
            sets.computeIfAbsent(key, k -> new ArrayList<>()).add(member);
            if (expiresAt > 0) setExpiry.put(key, expiresAt);
        }
        for (Map.Entry<String, List<String>> entry : sets.entrySet()) {
            String key = entry.getKey();
            super.sadd(key, entry.getValue().toArray(new String[0]));
            Long exp = setExpiry.get(key);
            if (exp != null) super.expire(key, Math.max(1L, (exp - now) / 1000L));
        }

        // --- sorted sets ---
        Map<String, Map<String, Double>> zsets = new LinkedHashMap<>();
        Map<String, Long> zsetExpiry = new HashMap<>();

        List<Map<String, Object>> zsetRows = store.selectAll(ZSETS_TABLE);
        for (Map<String, Object> row : zsetRows) {
            String key     = (String) row.get("key");
            String member  = (String) row.get("member");
            double score   = toDouble(row.get("score"));
            long expiresAt = toLong(row.get("expires_at"));
            if (expiresAt > 0 && expiresAt <= now) continue;
            zsets.computeIfAbsent(key, k -> new LinkedHashMap<>()).put(member, score);
            if (expiresAt > 0) zsetExpiry.put(key, expiresAt);
        }
        for (Map.Entry<String, Map<String, Double>> entry : zsets.entrySet()) {
            String key = entry.getKey();
            super.zadd(key, entry.getValue());
            Long exp = zsetExpiry.get(key);
            if (exp != null) super.expire(key, Math.max(1L, (exp - now) / 1000L));
        }

        // Clean up expired entries is handled by the background purge scheduler.
    }

    /**
     * Deletes all expired entries from every store table.
     * Called automatically at the end of {@link #warmUp()}; may also be called
     * explicitly at any time (e.g. on a scheduled basis) to reclaim store space.
     */
    public void purgeExpired() throws SQLException {
        long now = System.currentTimeMillis();

        // strings — PK: key
        for (Map<String, Object> row : store.selectAll(STRINGS_TABLE)) {
            if (isExpired(row.get("expires_at"), now)) {
                store.delete(STRINGS_TABLE, Map.of("key", row.get("key")));
            }
        }
        // hashes — PK: (hash_key, field)
        for (Map<String, Object> row : store.selectAll(HASHES_TABLE)) {
            if (isExpired(row.get("expires_at"), now)) {
                Map<String, Object> w = new HashMap<>();
                w.put("hash_key", row.get("hash_key"));
                w.put("field",    row.get("field"));
                store.delete(HASHES_TABLE, w);
            }
        }
        // lists — PK: (key, idx)
        for (Map<String, Object> row : store.selectAll(LISTS_TABLE)) {
            if (isExpired(row.get("expires_at"), now)) {
                Map<String, Object> w = new HashMap<>();
                w.put("key", row.get("key"));
                w.put("idx", row.get("idx"));
                store.delete(LISTS_TABLE, w);
            }
        }
        // sets — PK: (key, member)
        for (Map<String, Object> row : store.selectAll(SETS_TABLE)) {
            if (isExpired(row.get("expires_at"), now)) {
                Map<String, Object> w = new HashMap<>();
                w.put("key",    row.get("key"));
                w.put("member", row.get("member"));
                store.delete(SETS_TABLE, w);
            }
        }
        // zsets — PK: (key, member)
        for (Map<String, Object> row : store.selectAll(ZSETS_TABLE)) {
            if (isExpired(row.get("expires_at"), now)) {
                Map<String, Object> w = new HashMap<>();
                w.put("key",    row.get("key"));
                w.put("member", row.get("member"));
                store.delete(ZSETS_TABLE, w);
            }
        }
    }

    // -------------------------------------------------------------------------
    // String commands
    // -------------------------------------------------------------------------

    @Override
    public String set(final String key, final String value) {
        try {
            upsertString(key, value, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store write failed for key: " + key, e);
        }
        return super.set(key, value);
    }

    @Override
    public String setex(final String key, final long seconds, final String value) {
        long expiresAt = System.currentTimeMillis() + seconds * 1_000L;
        try {
            upsertString(key, value, expiresAt);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store setex failed for key: " + key, e);
        }
        return super.setex(key, seconds, value);
    }

    @Override
    public String psetex(final String key, final long milliseconds, final String value) {
        long expiresAt = System.currentTimeMillis() + milliseconds;
        try {
            upsertString(key, value, expiresAt);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store psetex failed for key: " + key, e);
        }
        return super.psetex(key, milliseconds, value);
    }

    @Override
    public String set(final String key, final String value, final SetParams params) {
        // Resolve TTL from NX/XX/EX/PX/EXAT/PXAT params.
        long expiresAt = resolveSetParamsExpiry(params);
        try {
            upsertString(key, value, expiresAt);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store set failed for key: " + key, e);
        }
        return super.set(key, value, params);
    }

    @Override
    public long setnx(final String key, final String value) {
        try {
            upsertString(key, value, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store setnx failed for key: " + key, e);
        }
        return super.setnx(key, value);
    }

    @Override
    public String mset(final String... keysvalues) {
        try {
            for (int i = 0; i < keysvalues.length - 1; i += 2) {
                upsertString(keysvalues[i], keysvalues[i + 1], 0L);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store mset failed", e);
        }
        return super.mset(keysvalues);
    }

    @Override
    public long msetnx(final String... keysvalues) {
        try {
            for (int i = 0; i < keysvalues.length - 1; i += 2) {
                upsertString(keysvalues[i], keysvalues[i + 1], 0L);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store msetnx failed", e);
        }
        return super.msetnx(keysvalues);
    }

    @Override
    public String getDel(final String key) {
        try {
            Map<String, Object> where = new HashMap<>();
            where.put("key", key);
            store.delete(STRINGS_TABLE, where);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store getDel failed for key: " + key, e);
        }
        return super.getDel(key);
    }

    @Override
    public String getSet(final String key, final String value) {
        try {
            upsertString(key, value, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store getSet failed for key: " + key, e);
        }
        return super.getSet(key, value);
    }

    // -------------------------------------------------------------------------
    // Key commands (del, expire)
    // -------------------------------------------------------------------------

    @Override
    public long del(final String key) {
        try {
            deleteKey(key);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store del failed for key: " + key, e);
        }
        return super.del(key);
    }

    @Override
    public long del(final String... keys) {
        try {
            for (String key : keys) {
                deleteKey(key);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store del failed", e);
        }
        return super.del(keys);
    }

    @Override
    public long expire(final String key, final long seconds) {
        long expiresAt = System.currentTimeMillis() + seconds * 1_000L;
        try {
            Map<String, Object> setExp = new HashMap<>();
            setExp.put("expires_at", expiresAt);

            Map<String, Object> whereStr = new HashMap<>();
            whereStr.put("key", key);
            store.update(STRINGS_TABLE, setExp, whereStr);

            Map<String, Object> whereHash = new HashMap<>();
            whereHash.put("hash_key", key);
            store.update(HASHES_TABLE, setExp, whereHash);

            store.update(LISTS_TABLE, setExp, whereStr);
            store.update(SETS_TABLE,  setExp, whereStr);

            Map<String, Object> whereZset = new HashMap<>();
            whereZset.put("key", key);
            store.update(ZSETS_TABLE, setExp, whereZset);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store expire failed for key: " + key, e);
        }
        return super.expire(key, seconds);
    }

    @Override
    public long pexpire(final String key, final long milliseconds) {
        long expiresAt = System.currentTimeMillis() + milliseconds;
        try {
            setExpiryAllTables(key, expiresAt);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store pexpire failed for key: " + key, e);
        }
        return super.pexpire(key, milliseconds);
    }

    @Override
    public long expireAt(final String key, final long unixTime) {
        long expiresAt = unixTime * 1_000L;
        try {
            setExpiryAllTables(key, expiresAt);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store expireAt failed for key: " + key, e);
        }
        return super.expireAt(key, unixTime);
    }

    @Override
    public long pexpireAt(final String key, final long millisecondsTimestamp) {
        try {
            setExpiryAllTables(key, millisecondsTimestamp);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store pexpireAt failed for key: " + key, e);
        }
        return super.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public long persist(final String key) {
        try {
            setExpiryAllTables(key, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store persist failed for key: " + key, e);
        }
        return super.persist(key);
    }

    @Override
    public long unlink(final String key) {
        try {
            deleteKey(key);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store unlink failed for key: " + key, e);
        }
        return super.unlink(key);
    }

    @Override
    public long unlink(final String... keys) {
        try {
            for (String key : keys) deleteKey(key);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store unlink failed", e);
        }
        return super.unlink(keys);
    }

    @Override
    public String rename(final String oldkey, final String newkey) {
        try {
            renameKey(oldkey, newkey);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store rename failed for key: " + oldkey, e);
        }
        return super.rename(oldkey, newkey);
    }

    @Override
    public long hset(final String key, final String field, final String value) {
        try {
            upsertHash(key, field, value, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store hset failed for key: " + key + ", field: " + field, e);
        }
        return super.hset(key, field, value);
    }

    @Override
    public long hset(final String key, final Map<String, String> hash) {
        try {
            for (Map.Entry<String, String> entry : hash.entrySet()) {
                upsertHash(key, entry.getKey(), entry.getValue(), 0L);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store hset failed for key: " + key, e);
        }
        return super.hset(key, hash);
    }

    @Override
    public long hdel(final String key, final String... fields) {
        try {
            for (String field : fields) {
                Map<String, Object> where = new HashMap<>();
                where.put("hash_key", key);
                where.put("field", field);
                store.delete(HASHES_TABLE, where);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store hdel failed for key: " + key, e);
        }
        return super.hdel(key, fields);
    }

    // -------------------------------------------------------------------------
    // List commands
    // -------------------------------------------------------------------------

    @Override
    public long rpush(final String key, final String... values) {
        try {
            appendListValues(key, values, false);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store rpush failed for key: " + key, e);
        }
        return super.rpush(key, values);
    }

    @Override
    public long lpush(final String key, final String... values) {
        try {
            // lpush prepends: for persistence, store the values in reverse order
            // relative to existing capacity. We model the list by idx; prepend
            // by inserting at negative indices before the current minimum.
            appendListValues(key, values, true);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store lpush failed for key: " + key, e);
        }
        return super.lpush(key, values);
    }

    @Override
    public String lset(final String key, final long index, final String value) {
        try {
            Map<String, Object> where = new HashMap<>();
            where.put("key", key);
            where.put("idx", index);
            store.delete(LISTS_TABLE, where);
            Map<String, Object> row = new HashMap<>();
            row.put("key",        key);
            row.put("idx",        index);
            row.put("value",      value);
            row.put("expires_at", 0L);
            store.insert(LISTS_TABLE, row);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store lset failed for key: " + key, e);
        }
        return super.lset(key, index, value);
    }

    @Override
    public long lpushx(final String key, final String... values) {
        try {
            if (!store.select(LISTS_TABLE, Map.of("key", key)).isEmpty()) {
                appendListValues(key, values, true);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store lpushx failed for key: " + key, e);
        }
        return super.lpushx(key, values);
    }

    @Override
    public long rpushx(final String key, final String... values) {
        try {
            if (!store.select(LISTS_TABLE, Map.of("key", key)).isEmpty()) {
                appendListValues(key, values, false);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store rpushx failed for key: " + key, e);
        }
        return super.rpushx(key, values);
    }

    @Override
    public String lpop(final String key) {
        try {
            removeListHead(key, 1);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store lpop failed for key: " + key, e);
        }
        return super.lpop(key);
    }

    @Override
    public List<String> lpop(final String key, final int count) {
        try {
            removeListHead(key, count);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store lpop failed for key: " + key, e);
        }
        return super.lpop(key, count);
    }

    @Override
    public String rpop(final String key) {
        try {
            removeListTail(key, 1);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store rpop failed for key: " + key, e);
        }
        return super.rpop(key);
    }

    @Override
    public List<String> rpop(final String key, final int count) {
        try {
            removeListTail(key, count);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store rpop failed for key: " + key, e);
        }
        return super.rpop(key, count);
    }

    @Override
    public long lrem(final String key, final long count, final String value) {
        // Mirror Redis lrem semantics: count>0 remove from head, count<0 from tail,
        // count=0 remove all. For store correctness we delete all matching rows;
        // Redis handles the count-based direction itself.
        try {
            List<Map<String, Object>> rows = store.select(LISTS_TABLE, Map.of("key", key));
            rows.sort(Comparator.comparingLong(r -> toLong(r.get("idx"))));
            int limit = (count == 0) ? rows.size() : (int) Math.abs(count);
            if (count < 0) java.util.Collections.reverse(rows);
            int removed = 0;
            for (Map<String, Object> row : rows) {
                if (removed >= limit) break;
                if (value.equals(row.get("value"))) {
                    Map<String, Object> w = new HashMap<>();
                    w.put("key", key);
                    w.put("idx", row.get("idx"));
                    store.delete(LISTS_TABLE, w);
                    removed++;
                }
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store lrem failed for key: " + key, e);
        }
        return super.lrem(key, count, value);
    }

    @Override
    public String ltrim(final String key, final long start, final long end) {
        try {
            List<Map<String, Object>> rows = store.select(LISTS_TABLE, Map.of("key", key));
            rows.sort(Comparator.comparingLong(r -> toLong(r.get("idx"))));
            int size = rows.size();
            long s = start < 0 ? Math.max(0, size + start) : start;
            long e = end   < 0 ? size + end               : Math.min(end, size - 1L);
            for (int i = 0; i < size; i++) {
                if (i < s || i > e) {
                    Map<String, Object> w = new HashMap<>();
                    w.put("key", key);
                    w.put("idx", rows.get(i).get("idx"));
                    store.delete(LISTS_TABLE, w);
                }
            }
        } catch (SQLException ex) {
            throw new JedisException("SyncLite store ltrim failed for key: " + key, ex);
        }
        return super.ltrim(key, start, end);
    }

    @Override
    public long sadd(final String key, final String... members) {
        try {
            for (String member : members) {
                // Idempotent: delete then insert to avoid duplicates.
                Map<String, Object> where = new HashMap<>();
                where.put("key",    key);
                where.put("member", member);
                store.delete(SETS_TABLE, where);
                Map<String, Object> row = new HashMap<>();
                row.put("key",        key);
                row.put("member",     member);
                row.put("expires_at", 0L);
                store.insert(SETS_TABLE, row);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store sadd failed for key: " + key, e);
        }
        return super.sadd(key, members);
    }

    @Override
    public long srem(final String key, final String... members) {
        try {
            for (String member : members) {
                Map<String, Object> where = new HashMap<>();
                where.put("key",    key);
                where.put("member", member);
                store.delete(SETS_TABLE, where);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store srem failed for key: " + key, e);
        }
        return super.srem(key, members);
    }

    @Override
    public String spop(final String key) {
        String popped = super.spop(key);
        if (popped != null) {
            try {
                Map<String, Object> w = new HashMap<>();
                w.put("key",    key);
                w.put("member", popped);
                store.delete(SETS_TABLE, w);
            } catch (SQLException e) {
                throw new JedisException("SyncLite store spop failed for key: " + key, e);
            }
        }
        return popped;
    }

    @Override
    public Set<String> spop(final String key, final long count) {
        Set<String> popped = super.spop(key, count);
        if (popped != null) {
            try {
                for (String member : popped) {
                    Map<String, Object> w = new HashMap<>();
                    w.put("key",    key);
                    w.put("member", member);
                    store.delete(SETS_TABLE, w);
                }
            } catch (SQLException e) {
                throw new JedisException("SyncLite store spop failed for key: " + key, e);
            }
        }
        return popped;
    }

    @Override
    public long smove(final String srckey, final String dstkey, final String member) {
        try {
            Map<String, Object> srcWhere = new HashMap<>();
            srcWhere.put("key",    srckey);
            srcWhere.put("member", member);
            store.delete(SETS_TABLE, srcWhere);

            Map<String, Object> dstRow = new HashMap<>();
            dstRow.put("key",        dstkey);
            dstRow.put("member",     member);
            dstRow.put("expires_at", 0L);
            // Idempotent: delete first in case member already exists in dst
            store.delete(SETS_TABLE, Map.of("key", dstkey, "member", member));
            store.insert(SETS_TABLE, dstRow);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store smove failed for src: " + srckey, e);
        }
        return super.smove(srckey, dstkey, member);
    }

    @Override
    public long zadd(final String key, final double score, final String member) {
        try {
            upsertZSet(key, member, score, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store zadd failed for key: " + key, e);
        }
        return super.zadd(key, score, member);
    }

    @Override
    public long zadd(final String key, final double score, final String member, final ZAddParams params) {
        try {
            upsertZSet(key, member, score, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store zadd failed for key: " + key, e);
        }
        return super.zadd(key, score, member, params);
    }

    @Override
    public long zadd(final String key, final Map<String, Double> scoreMembers) {
        try {
            for (Map.Entry<String, Double> e : scoreMembers.entrySet()) {
                upsertZSet(key, e.getKey(), e.getValue(), 0L);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store zadd failed for key: " + key, e);
        }
        return super.zadd(key, scoreMembers);
    }

    @Override
    public long zadd(final String key, final Map<String, Double> scoreMembers, final ZAddParams params) {
        try {
            for (Map.Entry<String, Double> e : scoreMembers.entrySet()) {
                upsertZSet(key, e.getKey(), e.getValue(), 0L);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store zadd failed for key: " + key, e);
        }
        return super.zadd(key, scoreMembers, params);
    }

    @Override
    public long zrem(final String key, final String... members) {
        try {
            for (String member : members) {
                Map<String, Object> where = new HashMap<>();
                where.put("key",    key);
                where.put("member", member);
                store.delete(ZSETS_TABLE, where);
            }
        } catch (SQLException e) {
            throw new JedisException("SyncLite store zrem failed for key: " + key, e);
        }
        return super.zrem(key, members);
    }

    @Override
    public double zincrby(final String key, final double increment, final String member) {
        double newScore = super.zincrby(key, increment, member);
        try {
            upsertZSet(key, member, newScore, 0L);
        } catch (SQLException e) {
            throw new JedisException("SyncLite store zincrby failed for key: " + key, e);
        }
        return newScore;
    }

    @Override
    public Tuple zpopmin(final String key) {
        Tuple t = super.zpopmin(key);
        if (t != null) {
            try {
                Map<String, Object> w = new HashMap<>();
                w.put("key",    key);
                w.put("member", t.getElement());
                store.delete(ZSETS_TABLE, w);
            } catch (SQLException e) {
                throw new JedisException("SyncLite store zpopmin failed for key: " + key, e);
            }
        }
        return t;
    }

    @Override
    public List<Tuple> zpopmin(final String key, final int count) {
        List<Tuple> tuples = super.zpopmin(key, count);
        if (tuples != null) {
            try {
                for (Tuple t : tuples) {
                    Map<String, Object> w = new HashMap<>();
                    w.put("key",    key);
                    w.put("member", t.getElement());
                    store.delete(ZSETS_TABLE, w);
                }
            } catch (SQLException e) {
                throw new JedisException("SyncLite store zpopmin failed for key: " + key, e);
            }
        }
        return tuples;
    }

    @Override
    public Tuple zpopmax(final String key) {
        Tuple t = super.zpopmax(key);
        if (t != null) {
            try {
                Map<String, Object> w = new HashMap<>();
                w.put("key",    key);
                w.put("member", t.getElement());
                store.delete(ZSETS_TABLE, w);
            } catch (SQLException e) {
                throw new JedisException("SyncLite store zpopmax failed for key: " + key, e);
            }
        }
        return t;
    }

    @Override
    public List<Tuple> zpopmax(final String key, final int count) {
        List<Tuple> tuples = super.zpopmax(key, count);
        if (tuples != null) {
            try {
                for (Tuple t : tuples) {
                    Map<String, Object> w = new HashMap<>();
                    w.put("key",    key);
                    w.put("member", t.getElement());
                    store.delete(ZSETS_TABLE, w);
                }
            } catch (SQLException e) {
                throw new JedisException("SyncLite store zpopmax failed for key: " + key, e);
            }
        }
        return tuples;
    }

    // -------------------------------------------------------------------------
    // Unsupported write operations
    //
    // These methods mutate Redis state in ways that cannot be durably mirrored
    // to the SyncLite backing store.  Calling them always throws
    // UnsupportedOperationException.
    // -------------------------------------------------------------------------

    // -- String in-place mutations --

    @Override
    public long append(final String key, final String value) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support append");
    }

    @Override
    public long setrange(final String key, final long offset, final String value) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support setrange");
    }

    @Override
    public long incr(final String key) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support incr");
    }

    @Override
    public long incrBy(final String key, final long increment) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support incrBy");
    }

    @Override
    public double incrByFloat(final String key, final double increment) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support incrByFloat");
    }

    @Override
    public long decr(final String key) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support decr");
    }

    @Override
    public long decrBy(final String key, final long decrement) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support decrBy");
    }

    // -- Hash in-place mutations --

    @Override
    public long hsetnx(final String key, final String field, final String value) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support hsetnx");
    }

    @Override
    public long hincrBy(final String key, final String field, final long value) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support hincrBy");
    }

    @Override
    public double hincrByFloat(final String key, final String field, final double value) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support hincrByFloat");
    }

    // -- List structural mutations --

    @Override
    public long linsert(final String key, final ListPosition where, final String pivot, final String value) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support linsert");
    }

    @Override
    public String lmove(final String srcKey, final String dstKey, final ListDirection from, final ListDirection to) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support lmove");
    }

    @Override
    public String blmove(final String srcKey, final String dstKey, final ListDirection from, final ListDirection to, final double timeout) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support blmove");
    }

    @Override
    public List<String> blpop(final int timeout, final String key) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support blpop");
    }

    @Override
    public List<String> blpop(final int timeout, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support blpop");
    }

    @Override
    public List<String> brpop(final int timeout, final String key) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support brpop");
    }

    @Override
    public List<String> brpop(final int timeout, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support brpop");
    }

    // -- Set aggregation-store ops --

    @Override
    public long sunionstore(final String dstkey, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support sunionstore");
    }

    @Override
    public long sinterstore(final String dstkey, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support sinterstore");
    }

    @Override
    public long sdiffstore(final String dstkey, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support sdiffstore");
    }

    // -- Sorted-set aggregation-store ops --

    @Override
    public long zdiffStore(final String dstKey, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support zdiffStore");
    }

    @Override
    public long zunionstore(final String dstKey, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support zunionstore");
    }

    @Override
    public long zunionstore(final String dstKey, final ZParams params, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support zunionstore");
    }

    @Override
    public long zinterstore(final String dstKey, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support zinterstore");
    }

    @Override
    public long zinterstore(final String dstKey, final ZParams params, final String... keys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support zinterstore");
    }

    @Override
    public long zrangestore(final String dest, final String src, final ZRangeParams zRangeParams) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support zrangestore");
    }
    // note: signature matches redis.clients.jedis.Jedis#zrangestore(String,String,ZRangeParams)

    // -- Key ops that cannot be reconciled with the store --

    @Override
    public long renamenx(final String oldkey, final String newkey) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support renamenx \u2014 use rename() instead");
    }

    @Override
    public boolean copy(final String srcKey, final String dstKey, final boolean replace) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support copy");
    }

    @Override
    public boolean copy(final String srcKey, final String dstKey, final int db, final boolean replace) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support copy");
    }

    @Override
    public long move(final String key, final int dbIndex) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support move");
    }

    @Override
    public String restore(final String key, final long ttl, final byte[] serializedValue) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support restore");
    }

    @Override
    public long sort(final String key, final SortingParams sortingParameters, final String dstkey) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support sort-with-store");
    }

    // -- Geo write ops --

    @Override
    public long geoadd(final String key, final double longitude, final double latitude, final String member) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support geoadd");
    }

    @Override
    public long geoadd(final String key, final Map<String, GeoCoordinate> memberCoordinateMap) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support geoadd");
    }

    @Override
    public long geoadd(final String key, final GeoAddParams params, final Map<String, GeoCoordinate> memberCoordinateMap) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support geoadd");
    }

    // -- HyperLogLog write ops --

    @Override
    public long pfadd(final String key, final String... elements) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support pfadd");
    }

    @Override
    public String pfmerge(final String destkey, final String... sourcekeys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support pfmerge");
    }

    // -- Bitmap write ops --

    @Override
    public boolean setbit(final String key, final long offset, final boolean value) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support setbit");
    }

    @Override
    public long bitop(final BitOP op, final String destKey, final String... srcKeys) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support bitop");
    }

    @Override
    public List<Long> bitfield(final String key, final String... arguments) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support bitfield");
    }

    // -- Stream write ops --

    @Override
    public StreamEntryID xadd(final String key, final StreamEntryID id, final Map<String, String> hash) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support xadd");
    }

    @Override
    public long xdel(final String key, final StreamEntryID... ids) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support xdel");
    }

    @Override
    public long xtrim(final String key, final long maxLen, final boolean approximate) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support xtrim");
    }

    @Override
    public long xack(final String key, final String group, final StreamEntryID... ids) {
        throw new UnsupportedOperationException("SyncLite-backed Jedis does not support xack");
    }

    /**
     * Closes the Redis connection and shuts down the background purge scheduler.
     * The {@link SyncLiteStore} is closed only when this instance created it via
     * managed builder methods.
     */
    @Override
    public void close() {
        purgeScheduler.shutdownNow();
        SQLException storeCloseError = null;
        try {
            if (managesStoreLifecycle) {
                try {
                    store.close();
                } catch (SQLException e) {
                    storeCloseError = e;
                }
                if (managedStoreDbPath != null) {
                    try {
                        SQLiteStore.closeDevice(managedStoreDbPath);
                    } catch (SQLException e) {
                        if (storeCloseError == null) {
                            storeCloseError = e;
                        }
                    }
                }
            }
        } finally {
            super.close();
        }

        if (storeCloseError != null) {
            throw new JedisException("Failed closing managed SyncLiteStore", storeCloseError);
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void upsertString(String key, String value, long expiresAt) throws SQLException {
        Map<String, Object> where = new HashMap<>();
        where.put("key", key);
        store.delete(STRINGS_TABLE, where);

        Map<String, Object> row = new HashMap<>();
        row.put("key",        key);
        row.put("value",      value);
        row.put("expires_at", expiresAt);
        store.insert(STRINGS_TABLE, row);
    }

    private void setExpiryAllTables(String key, long expiresAt) throws SQLException {
        Map<String, Object> setExp = new HashMap<>();
        setExp.put("expires_at", expiresAt);
        Map<String, Object> byKey = new HashMap<>();
        byKey.put("key", key);
        store.update(STRINGS_TABLE, setExp, byKey);
        store.update(LISTS_TABLE,   setExp, byKey);
        store.update(SETS_TABLE,    setExp, byKey);
        store.update(ZSETS_TABLE,   setExp, byKey);
        Map<String, Object> byHashKey = new HashMap<>();
        byHashKey.put("hash_key", key);
        store.update(HASHES_TABLE, setExp, byHashKey);
    }

    private void renameKey(String oldkey, String newkey) throws SQLException {
        // Strings
        List<Map<String, Object>> strRows = store.select(STRINGS_TABLE, Map.of("key", oldkey));
        if (!strRows.isEmpty()) {
            store.delete(STRINGS_TABLE, Map.of("key", oldkey));
            store.delete(STRINGS_TABLE, Map.of("key", newkey));
            Map<String, Object> r = new HashMap<>(strRows.get(0));
            r.put("key", newkey);
            store.insert(STRINGS_TABLE, r);
        }
        // Hashes
        List<Map<String, Object>> hashRows = store.select(HASHES_TABLE, Map.of("hash_key", oldkey));
        if (!hashRows.isEmpty()) {
            store.delete(HASHES_TABLE, Map.of("hash_key", newkey));
            for (Map<String, Object> row : hashRows) {
                store.delete(HASHES_TABLE, Map.of("hash_key", oldkey, "field", row.get("field")));
                Map<String, Object> r = new HashMap<>(row);
                r.put("hash_key", newkey);
                store.insert(HASHES_TABLE, r);
            }
        }
        // Lists
        List<Map<String, Object>> listRows = store.select(LISTS_TABLE, Map.of("key", oldkey));
        if (!listRows.isEmpty()) {
            store.delete(LISTS_TABLE, Map.of("key", newkey));
            for (Map<String, Object> row : listRows) {
                store.delete(LISTS_TABLE, Map.of("key", oldkey, "idx", row.get("idx")));
                Map<String, Object> r = new HashMap<>(row);
                r.put("key", newkey);
                store.insert(LISTS_TABLE, r);
            }
        }
        // Sets
        List<Map<String, Object>> setRows = store.select(SETS_TABLE, Map.of("key", oldkey));
        if (!setRows.isEmpty()) {
            store.delete(SETS_TABLE, Map.of("key", newkey));
            for (Map<String, Object> row : setRows) {
                store.delete(SETS_TABLE, Map.of("key", oldkey, "member", row.get("member")));
                Map<String, Object> r = new HashMap<>(row);
                r.put("key", newkey);
                store.insert(SETS_TABLE, r);
            }
        }
        // ZSets
        List<Map<String, Object>> zsetRows = store.select(ZSETS_TABLE, Map.of("key", oldkey));
        if (!zsetRows.isEmpty()) {
            store.delete(ZSETS_TABLE, Map.of("key", newkey));
            for (Map<String, Object> row : zsetRows) {
                store.delete(ZSETS_TABLE, Map.of("key", oldkey, "member", row.get("member")));
                Map<String, Object> r = new HashMap<>(row);
                r.put("key", newkey);
                store.insert(ZSETS_TABLE, r);
            }
        }
    }

    private void upsertHash(String hashKey, String field, String value, long expiresAt) throws SQLException {
        Map<String, Object> where = new HashMap<>();
        where.put("hash_key", hashKey);
        where.put("field",    field);
        store.delete(HASHES_TABLE, where);

        Map<String, Object> row = new HashMap<>();
        row.put("hash_key",   hashKey);
        row.put("field",      field);
        row.put("value",      value);
        row.put("expires_at", expiresAt);
        store.insert(HASHES_TABLE, row);
    }

    /**
     * Deletes a key from all five store tables.
     * Mirrors Redis {@code DEL} which removes any key type.
     */
    private void deleteKey(String key) throws SQLException {
        Map<String, Object> byKey = new HashMap<>();
        byKey.put("key", key);
        store.delete(STRINGS_TABLE, byKey);
        store.delete(LISTS_TABLE,   byKey);
        store.delete(SETS_TABLE,    byKey);
        store.delete(ZSETS_TABLE,   byKey);

        Map<String, Object> hashWhere = new HashMap<>();
        hashWhere.put("hash_key", key);
        store.delete(HASHES_TABLE, hashWhere);
    }

    /**
     * Appends list values to the store, assigning sequential idx values.
     * If {@code prepend} is true the values are assigned descending indices
     * below the current minimum (lpush semantics).
     */
    private void appendListValues(String key, String[] values, boolean prepend) throws SQLException {
        List<Map<String, Object>> existing = store.select(LISTS_TABLE, Map.of("key", key));
        long boundary;
        if (existing.isEmpty()) {
            boundary = prepend ? -1L : 0L;
        } else {
            if (prepend) {
                boundary = existing.stream().mapToLong(r -> toLong(r.get("idx"))).min().getAsLong() - 1;
            } else {
                boundary = existing.stream().mapToLong(r -> toLong(r.get("idx"))).max().getAsLong() + 1;
            }
        }
        for (String value : values) {
            Map<String, Object> row = new HashMap<>();
            row.put("key",        key);
            row.put("idx",        boundary);
            row.put("value",      value);
            row.put("expires_at", 0L);
            store.insert(LISTS_TABLE, row);
            boundary += prepend ? -1L : 1L;
        }
    }

    private void removeListHead(String key, int count) throws SQLException {
        List<Map<String, Object>> rows = store.select(LISTS_TABLE, Map.of("key", key));
        rows.sort(Comparator.comparingLong(r -> toLong(r.get("idx"))));
        int limit = Math.min(count, rows.size());
        for (int i = 0; i < limit; i++) {
            Map<String, Object> w = new HashMap<>();
            w.put("key", key);
            w.put("idx", rows.get(i).get("idx"));
            store.delete(LISTS_TABLE, w);
        }
    }

    private void removeListTail(String key, int count) throws SQLException {
        List<Map<String, Object>> rows = store.select(LISTS_TABLE, Map.of("key", key));
        rows.sort(Comparator.comparingLong(r -> toLong(r.get("idx"))));
        int size = rows.size();
        int limit = Math.min(count, size);
        for (int i = size - limit; i < size; i++) {
            Map<String, Object> w = new HashMap<>();
            w.put("key", key);
            w.put("idx", rows.get(i).get("idx"));
            store.delete(LISTS_TABLE, w);
        }
    }

    private void upsertZSet(String key, String member, double score, long expiresAt) throws SQLException {
        Map<String, Object> where = new HashMap<>();
        where.put("key",    key);
        where.put("member", member);
        store.delete(ZSETS_TABLE, where);

        Map<String, Object> row = new HashMap<>();
        row.put("key",        key);
        row.put("score",      score);
        row.put("member",     member);
        row.put("expires_at", expiresAt);
        store.insert(ZSETS_TABLE, row);
    }

    private static long toLong(Object o) {
        if (o == null) return 0L;
        return ((Number) o).longValue();
    }

    private static double toDouble(Object o) {
        if (o == null) return 0.0;
        return ((Number) o).doubleValue();
    }

    private static boolean isExpired(Object expiresAtObj, long now) {
        long exp = toLong(expiresAtObj);
        return exp > 0 && exp <= now;
    }

    private static long resolveSetParamsExpiry(SetParams params) {
        if (params == null) return 0L;
        try {
            // SetParams stores values under the Redis keyword keys (ex, px, exat, pxat).
            // The inherited getParam(String) method retrieves them directly, avoiding
            // any dependency on toString() format which varies across Jedis versions.
            Object pxat = params.getParam("pxat");
            if (pxat != null) return toLong(pxat);

            Object exat = params.getParam("exat");
            if (exat != null) return toLong(exat) * 1_000L;

            Object px = params.getParam("px");
            if (px != null) return System.currentTimeMillis() + toLong(px);

            Object ex = params.getParam("ex");
            if (ex != null) return System.currentTimeMillis() + toLong(ex) * 1_000L;
        } catch (Exception ignored) {}
        return 0L;
    }
}
