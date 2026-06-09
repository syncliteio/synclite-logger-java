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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Resolves and loads the SyncLite JNI native library on first touch.
 *
 * <p>The canonical jar layout matches the release zip's {@code lib/native/}
 * folder so the same artifact name flows everywhere:
 * <pre>
 *   META-INF/native/&lt;classifier&gt;/libsynclite_&lt;revision&gt;_jni.&lt;ext&gt;
 *   META-INF/native/&lt;classifier&gt;/[lib]duckdb.&lt;ext&gt;   (when present)
 * </pre>
 * where {@code <revision>} comes from the {@code jni.revision} key of
 * {@code META-INF/synclite-native.properties} (filtered at build time
 * with the Maven {@code ${revision}} property, e.g. {@code oss}).
 *
 * <p>Resolution order:
 * <ol>
 *   <li>{@code -Dsynclite.native.path=<path-to-lib>} (explicit override)</li>
 *   <li>Bundled classpath resource described above (the usual path; copied
 *       in by the {@code synclite} jar's antrun step).</li>
 *   <li>{@code System.loadLibrary(...)} (relies on {@code java.library.path}
 *       and OS lib-search rules).</li>
 * </ol>
 */
final class NativeLoader {

    private static final AtomicBoolean LOADED = new AtomicBoolean(false);
    private static final String PROPS_RESOURCE = "META-INF/synclite-native.properties";
    private static final String DEFAULT_REVISION = "oss";

    private NativeLoader() {}

    static void ensureLoaded() {
        if (LOADED.get()) {
            return;
        }
        synchronized (NativeLoader.class) {
            if (LOADED.get()) {
                return;
            }
            doLoad();
            LOADED.set(true);
        }
    }

    private static void doLoad() {
        String override = System.getProperty("synclite.native.path");
        if (override != null && !override.isEmpty()) {
            System.load(Paths.get(override).toAbsolutePath().toString());
            return;
        }

        String revision = loadRevision();
        String classifier = detectClassifier();
        String jniBase = "libsynclite_" + revision + "_jni";
        String jniName = osFileName(jniBase);
        // duckdb's import name varies by OS (cargo / duckdb-rs uses
        // duckdb.dll on Windows but libduckdb.{so,dylib} on Linux/macOS).
        // The synclite_jni binary's import table references that exact
        // name, so keep it OS-natural.
        String duckdbName = osDuckdbName();
        String jniResource = "META-INF/native/" + classifier + "/" + jniName;
        String duckdbResource = "META-INF/native/" + classifier + "/" + duckdbName;

        Path extractedJni = extractIfPresent(jniResource, jniName);
        if (extractedJni != null) {
            // synclite_jni dynamically links to libduckdb. Extract it into
            // the same temp dir and System.load() it FIRST so the OS loader
            // can satisfy the dependency before we try to load synclite_jni.
            Path tmpDir = extractedJni.getParent();
            Path extractedDuckdb = extractIfPresent(duckdbResource, duckdbName, tmpDir);
            if (extractedDuckdb != null) {
                System.load(extractedDuckdb.toAbsolutePath().toString());
            }
            System.load(extractedJni.toAbsolutePath().toString());
            return;
        }

        // Fall back to OS lib search using the canonical base name
        // (java.library.path or LD_LIBRARY_PATH / PATH / @rpath).
        try {
            System.loadLibrary(jniBase);
        } catch (UnsatisfiedLinkError e) {
            throw new SyncLiteException(
                    "native library '" + jniBase + "' not found. Searched (in order): "
                    + "-Dsynclite.native.path, classpath:" + jniResource
                    + ", java.library.path. Build it via "
                    + "`cargo build -p synclite-bindings-java` and either add the "
                    + "target dir to java.library.path or set -Dsynclite.native.path.",
                    e);
        }
    }

    private static String loadRevision() {
        try (InputStream in = NativeLoader.class.getClassLoader().getResourceAsStream(PROPS_RESOURCE)) {
            if (in == null) {
                return DEFAULT_REVISION;
            }
            Properties p = new Properties();
            p.load(in);
            String rev = p.getProperty("jni.revision", DEFAULT_REVISION).trim();
            return rev.isEmpty() ? DEFAULT_REVISION : rev;
        } catch (IOException e) {
            return DEFAULT_REVISION;
        }
    }

    private static Path extractIfPresent(String resourcePath, String fileName) {
        try {
            Path tmpDir = Files.createTempDirectory("synclite-native-");
            tmpDir.toFile().deleteOnExit();
            return extractIfPresent(resourcePath, fileName, tmpDir);
        } catch (IOException e) {
            throw new SyncLiteException(
                    "failed to create temp dir for bundled native library " + resourcePath, e);
        }
    }

    private static Path extractIfPresent(String resourcePath, String fileName, Path tmpDir) {
        try (InputStream in = NativeLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                return null;
            }
            Path tmpFile = tmpDir.resolve(fileName);
            Files.copy(in, tmpFile, StandardCopyOption.REPLACE_EXISTING);
            tmpFile.toFile().deleteOnExit();
            return tmpFile;
        } catch (IOException e) {
            throw new SyncLiteException(
                    "failed to extract bundled native library " + resourcePath, e);
        }
    }

    static String detectClassifier() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

        boolean isArm64 = arch.contains("aarch64") || arch.contains("arm64");

        if (os.contains("win")) {
            return "windows-x64";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return isArm64 ? "macos-arm64" : "macos-x64";
        }
        // Default to Linux family for everything else (Linux, BSDs).
        return isArm64 ? "linux-arm64" : "linux-x64";
    }

    /**
     * For a base name like {@code "libsynclite_oss_jni"} or {@code "libduckdb"},
     * returns the on-disk filename for the current OS. The base name (including
     * any "lib" prefix) is preserved verbatim on every platform; only the OS
     * extension is appended. This matches what the antrun copy step in
     * {@code synclite-logger-java/logger/pom.xml} writes into the jar and what
     * the release zip's {@code lib/native/} folder ships
     * (e.g. {@code libsynclite_oss.dll} on Windows — same {@code lib} prefix
     * as on Linux/macOS).
     */
    private static String osFileName(String baseWithLibPrefix) {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            return baseWithLibPrefix + ".dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return baseWithLibPrefix + ".dylib";
        }
        return baseWithLibPrefix + ".so";
    }

    /**
     * duckdb's prebuilt zips ship {@code duckdb.dll} on Windows but
     * {@code libduckdb.{so,dylib}} on Linux/macOS. {@code synclite_jni}'s
     * import table references that exact name on each platform, so we must
     * extract the file under its OS-natural name (no rename).
     */
    private static String osDuckdbName() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            return "duckdb.dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return "libduckdb.dylib";
        }
        return "libduckdb.so";
    }
}
