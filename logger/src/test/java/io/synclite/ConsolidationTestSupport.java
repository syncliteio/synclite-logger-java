/*
 * Copyright (c) 2025 mahendra.chavan@synclite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.synclite;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Shared utilities for the consolidator-module JUnit tests. Mirrors the
 * helper pattern used by the Rust {@code device_integration.rs} suite:
 * each test names the destination table it just wrote and the row count
 * it expects, and this class blocks until the in-process consolidator
 * has applied that state to the SQLite destination DB.
 *
 * <p>Layout — job-scoped stage/work dirs (shared across every test
 * in this suite) plus a per-test DB folder, so the destination DB is
 * a single artifact whose history reflects the order in which tests
 * ran. The job folder lives under {@code ~/synclite/test/<jobName>}
 * so production runs (which default to {@code ~/synclite/job1}) never
 * collide with test artifacts. Mirrors the Rust
 * {@code device_integration.rs} layout (job: {@code rustruntime}) and
 * the logger-module tests (job: {@code javalogger}):
 * <pre>
 *   ~/synclite/test/javaloggerconsolidator/db/&lt;TestName&gt;/
 *       test-*.db                                &lt;-- source device DB
 *       synclite.conf
 *   ~/synclite/test/javaloggerconsolidator/stageDir/
 *       synclite-&lt;device&gt;-&lt;uuid&gt;/             &lt;-- one per device
 *           *.sqllog ...
 *   ~/synclite/test/javaloggerconsolidator/workDir/
 *       consolidated_db.sqlite                   &lt;-- shared destination
 *       synclite_consolidator_statistics.db
 *       synclite_device_metadata.db
 *       synclite-&lt;device&gt;-&lt;uuid&gt;/             &lt;-- per-device work area
 * </pre>
 *
 * <p>Production defaults are unchanged ({@code ~/synclite/job1/stageDir}
 * and {@code ~/synclite/job1/workDir}, mirroring the Rust
 * {@code default_local_stage_dir} / {@code default_device_data_root}).
 */
public final class ConsolidationTestSupport {

    /** Job-name root for this test suite: {@code ~/synclite/test/javaloggerconsolidator}. */
    public static Path jobRoot() {
        return Path.of(System.getProperty("user.home"))
                .resolve("synclite").resolve("test").resolve("javaloggerconsolidator");
    }

    /** Per-test DB root: {@code <jobRoot>/db/<testName>}. */
    public static Path testRoot(String testName) {
        return jobRoot().resolve("db").resolve(testName);
    }

    /** Shared logger stage dir for this job: {@code <jobRoot>/stageDir}. */
    public static Path stageDir() {
        return jobRoot().resolve("stageDir");
    }

    /** Shared consolidator work dir for this job: {@code <jobRoot>/workDir}. */
    public static Path workDir() {
        return jobRoot().resolve("workDir");
    }

    /** Shared SQLite destination DB for this job: {@code <jobRoot>/workDir/consolidated_db.sqlite}. */
    public static Path destDb() {
        return workDir().resolve("consolidated_db.sqlite");
    }

    /**
     * Default SQLite destination options for a test. All tests in this
     * job point at the same {@link #destDb()} so behavior matches a
     * real deployment where one destination receives writes from every
     * source device.
     */
    public static DestinationOptions sqliteDestination(String testName) throws IOException {
        Files.createDirectories(workDir());
        String url = "jdbc:sqlite:" + destDb().toString().replace('\\', '/')
                + "?journal_mode=WAL";
        return DestinationOptions.builder()
                .dstType(DstType.SQLITE)
                .syncMode(DstSyncMode.CONSOLIDATION)
                .connectionString(url)
                .build();
    }

    /**
     * Wipe the per-test DB folder entirely and recreate the empty
     * per-test root, then make sure the shared stage / work dirs exist.
     * The {@code synclite-<device>-<uuid>} subfolder under the shared
     * stage / work dir belongs to a specific device UUID — each new
     * call to {@code initialize} writes into a fresh UUID folder, so
     * tests do not collide there either. The shared
     * {@code consolidated_db.sqlite} is left in place so its history
     * reflects the order in which tests ran.
     */
    public static void resetTestDirs(String testName, String deviceName) throws IOException {
        Path root = testRoot(testName);
        if (Files.exists(root)) {
            deleteRecursively(root);
        }
        Files.createDirectories(root);
        Files.createDirectories(stageDir());
        Files.createDirectories(workDir());
    }

    /**
     * Write the standard {@code synclite.conf} the consolidator tests
     * use. Mirrors the logger-module tests' two-line conf so the Java
     * logger does plain FS shipping into the shared job stage dir; the
     * consolidator's work-dir root is pinned via {@code device-data-root}
     * so destination state lives under the same per-job folder.
     *
     * <p>The consolidator's destination is supplied separately via
     * {@link DestinationOptions} and does NOT belong in this file —
     * adding {@code dst-*} keys here makes the logger think it has a
     * destination of its own and suppresses FS shipping.
     */
    public static Path writeConfig(String testName) throws IOException {
        Path conf = testRoot(testName).resolve("synclite.conf");
        // Java Properties.load treats `\` as an escape; write paths with
        // forward slashes (Windows accepts them) so neither the logger's
        // conf parser nor the consolidator's workDirFromProperties helper
        // mangles drive-letter paths.
        String stage = stageDir().toString().replace('\\', '/');
        String work  = workDir().toString().replace('\\', '/');
        String content =
                "local-data-stage-directory = " + stage + "\n" +
                "device-stage-type = FS\n" +
                "device-data-root = " + work + "\n";
        Files.writeString(conf, content);
        return conf;
    }

    /**
     * Block until the consolidator drains all in-flight CDC segments
     * and the destination table reaches {@code expectedRowCount}.
     * Mirrors Rust's {@code wait_for_sql_device_consolidation_and_validate_expected_count}.
     */
    public static void awaitAndAssertDestinationRowCount(
            String testName,
            Path srcDbPath,
            String table,
            long expectedRowCount,
            Duration timeout) throws SQLException {
        SyncLite.awaitSync(srcDbPath, timeout);

        Path dest = destDb();
        Instant deadline = Instant.now().plus(timeout);
        long observed = -1;
        SQLException lastErr = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                observed = countRows(dest, table);
                if (observed == expectedRowCount) {
                    return;
                }
                lastErr = null;
            } catch (SQLException e) {
                lastErr = e;
            }
            try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        if (lastErr != null) {
            fail("destination row-count probe failed for table " + table + " in " + dest + ": " + lastErr.getMessage());
        }
        fail("timeout waiting for destination row count of " + table
                + " in " + dest + ": expected " + expectedRowCount + ", observed " + observed);
    }

    /**
     * Same as {@link #awaitAndAssertDestinationRowCount} but uses a
     * 30-second default timeout.
     */
    public static void awaitAndAssertDestinationRowCount(
            String testName, Path srcDbPath, String table, long expectedRowCount) throws SQLException {
        awaitAndAssertDestinationRowCount(testName, srcDbPath, table, expectedRowCount, Duration.ofSeconds(30));
    }

    /**
     * Same as {@link #awaitAndAssertDestinationRowCount} but only
     * verifies the destination table exists (any row count is
     * accepted, including zero). Useful for tests whose final logical
     * state is an empty table (DROP / DELETE ALL).
     */
    public static void awaitAndAssertDestinationTableExists(
            String testName, Path srcDbPath, String table, Duration timeout) throws SQLException {
        SyncLite.awaitSync(srcDbPath, timeout);
        Path dest = destDb();
        Instant deadline = Instant.now().plus(timeout);
        SQLException lastErr = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                if (tableExists(dest, table)) {
                    return;
                }
                lastErr = null;
            } catch (SQLException e) {
                lastErr = e;
            }
            try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        if (lastErr != null) {
            fail("destination existence probe failed for table " + table + " in " + dest + ": " + lastErr.getMessage());
        }
        fail("timeout waiting for destination table " + table + " to appear in " + dest);
    }

    private static long countRows(Path dbFile, String table) throws SQLException {
        // Quote with double quotes to match how the consolidator names
        // case-preserving destination tables; SQLite tolerates this for
        // unquoted ASCII names too.
        String url = "jdbc:sqlite:" + dbFile.toString().replace('\\', '/');
        String sql = "SELECT COUNT(*) FROM \"" + table + "\"";
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "COUNT(*) row missing");
            return rs.getLong(1);
        }
    }

    private static boolean tableExists(Path dbFile, String table) throws SQLException {
        String url = "jdbc:sqlite:" + dbFile.toString().replace('\\', '/');
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table
                             + "' COLLATE NOCASE")) {
            return rs.next();
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (Files.notExists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    private ConsolidationTestSupport() {}
}
