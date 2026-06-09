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
 * Snapshot of consolidator counters for a device, as written to the
 * in-process consolidator's {@code synclite_consolidator_statistics.db}.
 * Returned by {@link SyncLite#syncStatistics(java.nio.file.Path)}.
 */
public final class SyncStatistics {

    private final long logSegmentsApplied;
    private final long processedOperCount;
    private final long processedTxnCount;
    private final long processedLogSize;
    private final long lastConsolidatedCommitId;
    private final long lastHeartbeatTimeMs;

    SyncStatistics(long logSegmentsApplied, long processedOperCount, long processedTxnCount,
                   long processedLogSize, long lastConsolidatedCommitId, long lastHeartbeatTimeMs) {
        this.logSegmentsApplied = logSegmentsApplied;
        this.processedOperCount = processedOperCount;
        this.processedTxnCount = processedTxnCount;
        this.processedLogSize = processedLogSize;
        this.lastConsolidatedCommitId = lastConsolidatedCommitId;
        this.lastHeartbeatTimeMs = lastHeartbeatTimeMs;
    }

    /** Number of log segments applied to the destination so far. */
    public long logSegmentsApplied() { return logSegmentsApplied; }

    /** Total row-level operations (insert + update + delete) applied. */
    public long processedOperCount() { return processedOperCount; }

    /** Total transactions applied. */
    public long processedTxnCount() { return processedTxnCount; }

    /** Total log-segment bytes processed. */
    public long processedLogSize() { return processedLogSize; }

    /** Last commit id applied at the destination. */
    public long lastConsolidatedCommitId() { return lastConsolidatedCommitId; }

    /** Epoch ms of the consolidator's last heartbeat update. */
    public long lastHeartbeatTimeMs() { return lastHeartbeatTimeMs; }

    @Override
    public String toString() {
        return "SyncStatistics{logSegmentsApplied=" + logSegmentsApplied
                + ", processedOperCount=" + processedOperCount
                + ", processedTxnCount=" + processedTxnCount
                + ", processedLogSize=" + processedLogSize
                + ", lastConsolidatedCommitId=" + lastConsolidatedCommitId
                + ", lastHeartbeatTimeMs=" + lastHeartbeatTimeMs + '}';
    }
}
