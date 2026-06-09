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

/**
 * Package-private JNI surface implemented by the {@code synclite_jni}
 * Rust crate ({@code crates/logger/bindings-java}). End users do NOT
 * call these methods directly; everything is funneled through
 * {@link SyncLite}. The class exists separately so {@link NativeLoader}
 * can initialize lazily on first access.
 */
final class NativeConsolidator {

    private NativeConsolidator() {}

    static {
        NativeLoader.ensureLoaded();
    }

    /**
     * Build a {@code ConsolidatorLayout} from the given fields, call
     * {@code Consolidator::spawn}, and return an opaque handle. All
     * string args are non-null except {@code dstDatabase} / {@code dstSchema}
     * which may be {@code null} when the destination does not need them.
     */
    static native long nativeSpawnConsolidator(
            String workDir,
            String deviceDataRoot,
            String deviceId,
            String deviceName,
            String deviceType,
            String databaseName,
            String dstType,
            String dstConnectionString,
            String dstSyncMode,
            String dstDatabase,
            String dstSchema,
            String metadataStore,
            String stageDir,
            long devicePollingIntervalMs);

    /** Notify the consolidator that a new segment has landed at {@code stagePath}. */
    static native void nativeNotifyStagePath(long handle, String stagePath);

    /**
     * Notify the consolidator that the bootstrap snapshot is available so
     * it can complete destination initialization from {@code backupPath}
     * (the {@code <db>.synclite.backup} snapshot the logger wrote) and
     * the matching {@code metadataPath} ({@code <db>.synclite.metadata}).
     * Must be called once per device before any segments can be applied;
     * stage-path notifications received earlier are buffered and drained
     * once bootstrap completes.
     */
    static native void nativeNotifyBootstrapReady(long handle, String backupPath, String metadataPath);

    /** Sweep {@code stageDir} once and enqueue every existing segment. */
    static native void nativeCatchUpStageDir(long handle, String stageDir);

    /**
     * Stop the consolidator worker and free the handle. Idempotent:
     * a zero/null handle is a no-op.
     */
    static native void nativeStopConsolidator(long handle);

    // ---------- path-based control / inspection -------------------------

    /** Pause destination consolidation for the device at {@code dbPath}. */
    static native void nativePauseSync(String dbPath);

    /** Resume destination consolidation for the device at {@code dbPath}. */
    static native void nativeResumeSync(String dbPath);

    /** {@code true} when a pause sentinel exists for the device at {@code dbPath}. */
    static native boolean nativeIsSyncPaused(String dbPath);

    /**
     * Wipe per-device local state (device home, stage subdir, work
     * subdir, segment counters) and — when reachable — delete this
     * device's rows from the destination metadata tables. A sentinel
     * file dropped under the device home causes the next
     * {@link io.synclite.SyncLite#initialize} to force
     * {@code dst-object-init-mode-1=OVERWRITE_OBJECT} for the
     * post-reinit re-seed only — REPLICATION drops and recreates the
     * destination tables, CONSOLIDATION truncates this device's rows
     * on the shared destination. Device UUID, name, type and
     * destination wiring are preserved so the same logical device
     * comes back up with a fresh segment sequence.
     */
    static native void nativeReinitialize(String dbPath);

    /**
     * Block until the consolidator has applied every commit produced by
     * the device, or {@code timeoutMs} elapses.
     */
    static native void nativeAwaitSync(String dbPath, long timeoutMs);

    /**
     * Block until the consolidator's applied commit-id reaches
     * {@code targetCommitId}, or {@code timeoutMs} elapses. The caller
     * supplies the target because the Rust runtime cannot crack open
     * the source DB for JDBC-bridge backends (Derby / H2 / HyperSQL)
     * where {@code synclite_txn} lives inside the backend's own DB
     * file. A target of 0 / negative is treated as a no-wait success.
     */
    static native void nativeAwaitAppliedCommit(String dbPath, long targetCommitId, long timeoutMs);

    /**
     * Returns {@code [Integer stateOrdinal, String status, String statusDescription,
     * Long lastHeartbeatTimeMs]}. {@code stateOrdinal} matches the ordinal
     * of {@link SyncState} (0 = NOT_INITIALIZED, 1 = PAUSED, 2 = RUNNING).
     */
    static native Object[] nativeSyncStatus(String dbPath);

    /**
     * Returns a 6-long array of consolidator counters in this order:
     * {@code [logSegmentsApplied, processedOperCount, processedTxnCount,
     * processedLogSize, lastConsolidatedCommitId, lastHeartbeatTimeMs]}.
     */
    static native long[] nativeSyncStatistics(String dbPath);

    /**
     * Returns {@code [sourceCommitId, appliedCommitIdOrLongMin, latencyMs]}.
     * {@code Long.MIN_VALUE} in slot 1 means the consolidator has not yet
     * recorded an applied commit; in that case {@code latencyMs} is {@code -1}.
     */
    static native long[] nativeSyncLatency(String dbPath);
}
