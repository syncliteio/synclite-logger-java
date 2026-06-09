/*
 * Copyright (c) 2025 mahendra.chavan@syncLite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.synclite;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

/**
 * Wipes the per-job test directories once per JVM, before any test runs.
 * Registered via {@code META-INF/services/org.junit.platform.launcher.LauncherSessionListener}.
 * Without this, the shared {@code workDir/consolidated_db.sqlite} accumulates
 * rows across tests and consolidator assertions like
 * "expected 4, observed 10" start failing.
 */
public final class TestJobRootCleaner implements LauncherSessionListener {

    @Override
    public void launcherSessionOpened(LauncherSession session) {
        Path testHome = Path.of(System.getProperty("user.home"))
                .resolve("synclite").resolve("test");
        wipe(testHome.resolve("javalogger"));
        wipe(testHome.resolve("javaloggerconsolidator"));
    }

    private static void wipe(Path dir) {
        if (Files.notExists(dir)) {
            return;
        }
        try {
            deleteRecursively(dir);
        } catch (IOException e) {
            System.err.println("[TestJobRootCleaner] failed to wipe " + dir + ": " + e);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (Stream<Path> children = Files.list(path)) {
                for (Path child : (Iterable<Path>) children::iterator) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
