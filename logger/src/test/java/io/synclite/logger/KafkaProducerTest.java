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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaProducerTest {

    private Path testDbPath;
    private Path testStageDir;
    private KafkaProducer producer;

    @BeforeEach
    void setUp() throws Exception {
        Path testHome = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("test");
        testDbPath = testHome.resolve("db").resolve("KafkaProducerTest");
        testStageDir = testHome.resolve("stageDir");

        if (Files.exists(testDbPath)) {
            deleteRecursively(testDbPath);
        }
        if (Files.exists(testStageDir)) {
            try (var stageDirs = Files.list(testStageDir)) {
                stageDirs.filter(p -> p.getFileName().toString().startsWith("synclite-default-"))
                         .forEach(p -> { try { deleteRecursively(p); } catch (java.io.IOException ignored) {} });
            }
        }

        Files.createDirectories(testDbPath);
        Files.createDirectories(testStageDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
            producer = null;
        }

        // Give Windows some time to release locks
        Thread.sleep(150);

        Path testHome = testStageDir.getParent();
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + testHome + "\n");
    }

    private void deleteRecursively(Path path) throws IOException {
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

    @Test
    void testSendMessages() throws Exception {
        // Build producer properties - KafkaProducer uses STREAMING device internally
        Properties props = new Properties();
        props.setProperty("device-path", testDbPath.toString());
        props.setProperty("local-data-stage-directory", testStageDir.toString());
        props.setProperty("destination-type", "FS");

        producer = new KafkaProducer(props);

        String topic = "test_topic";

        // First batch: send 2 messages
        Future<RecordMetadata> f1 = producer.send(new ProducerRecord<>(topic, "key1", "value1"));
        Future<RecordMetadata> f2 = producer.send(new ProducerRecord<>(topic, "key2", "value2"));

        RecordMetadata m1 = f1.get();
        RecordMetadata m2 = f2.get();

        assertNotNull(m1, "RecordMetadata for first message should not be null");
        assertNotNull(m2, "RecordMetadata for second message should not be null");
        assertEquals(topic, m1.topic(), "Topic should match for first message");
        assertEquals(topic, m2.topic(), "Topic should match for second message");
        // offset is derived from operationId - 2, which may be 0 or negative for the first ops;
        // just verify second message offset is greater than or equal to first (monotonically increasing)
        assertTrue(m2.offset() >= m1.offset(), "Second message offset should be >= first message offset");

        producer.flush();

        // Second batch: send 2 more messages
        Future<RecordMetadata> f3 = producer.send(new ProducerRecord<>(topic, "key3", "value3"));
        Future<RecordMetadata> f4 = producer.send(new ProducerRecord<>(topic, "key4", "value4"));

        f3.get();
        f4.get();

        producer.flush();

        // Verify unsupported transaction operations throw IllegalStateException
        assertThrows(IllegalStateException.class, () -> producer.initTransactions(),
                "initTransactions should throw IllegalStateException");
        assertThrows(IllegalStateException.class, () -> producer.beginTransaction(),
                "beginTransaction should throw IllegalStateException");
        assertThrows(IllegalStateException.class, () -> producer.commitTransaction(),
                "commitTransaction should throw IllegalStateException");
        assertThrows(IllegalStateException.class, () -> producer.abortTransaction(),
                "abortTransaction should throw IllegalStateException");

        // Close producer and flush the device to stage directory
        producer.close();
        producer = null;

        // Give Windows some time to release locks
        Thread.sleep(150);

        // The streaming device file is default.db inside testDbPath
        Path deviceFilePath = testDbPath.resolve("default.db");

        // Read the max commit_id from synclite_txn in the device file
        long commitId;
        try (Connection devConn = DriverManager.getConnection("jdbc:sqlite:" + deviceFilePath);
             Statement devStmt = devConn.createStatement();
             ResultSet devRs = devStmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
            assertTrue(devRs.next(), "Should have commit ID in synclite_txn table");
            commitId = devRs.getLong(1);
            assertTrue(commitId > 0, "Commit ID should be positive");
        }

        // Validate that .sqllog files were created in stage directory
        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

        Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
        Path latestLogFile = null;
        long latestMtime = -1;

        Path deviceStageDir = Files.list(testStageDir).filter(p -> p.getFileName().toString().startsWith("synclite-default-")).findFirst().orElse(testStageDir);
        try (var files = Files.walk(deviceStageDir)) {
            for (Path path : files.collect(Collectors.toList())) {
                if (!Files.isRegularFile(path)) continue;
                if (!sqllogPattern.matcher(path.getFileName().toString()).matches()) continue;
                long mtime = Files.getLastModifiedTime(path).toMillis();
                if (mtime > latestMtime) {
                    latestMtime = mtime;
                    latestLogFile = path;
                }
            }
        }

        assertNotNull(latestLogFile, "At least one .sqllog file should be created in stageDir");

        // Verify the commit_id from synclite_txn matches the last logged transaction in the stage log
        try (Connection logConn = DriverManager.getConnection("jdbc:sqlite:" + latestLogFile);
             PreparedStatement logStmt = logConn.prepareStatement(
                     "SELECT COUNT(*) as cnt FROM commandlog WHERE commit_id = ?")) {
            logStmt.setLong(1, commitId);
            try (ResultSet logRs = logStmt.executeQuery()) {
                assertTrue(logRs.next(), "Stage log query must return a result");
                assertTrue(logRs.getInt("cnt") > 0,
                        "Stage log file must contain an entry for commit_id " + commitId);
            }
        }
    }
}
