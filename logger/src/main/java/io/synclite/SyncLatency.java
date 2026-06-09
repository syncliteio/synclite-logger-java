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

import java.util.OptionalLong;

/**
 * Snapshot of sync lag between the device and the destination.
 * Returned by {@link SyncLite#syncLatency(java.nio.file.Path)}.
 *
 * <p>Both commit ids are {@code System.currentTimeMillis()}-style
 * wall-clock millisecond timestamps the logger emits, so
 * {@link #latencyMs()} is the wall-clock sync lag in milliseconds.
 */
public final class SyncLatency {

    private final long sourceCommitId;
    private final OptionalLong appliedCommitId;
    private final long latencyMs;

    SyncLatency(long sourceCommitId, OptionalLong appliedCommitId, long latencyMs) {
        this.sourceCommitId = sourceCommitId;
        this.appliedCommitId = appliedCommitId == null ? OptionalLong.empty() : appliedCommitId;
        this.latencyMs = latencyMs;
    }

    /**
     * {@code MAX(commit_id)} from the device's {@code synclite_txn} table.
     * {@code 0} if no user writes have committed yet.
     */
    public long sourceCommitId() {
        return sourceCommitId;
    }

    /**
     * Last commit id known to be applied at the destination. Empty when
     * the consolidator has not yet produced a heartbeat (destination
     * unreachable or consolidator not running).
     */
    public OptionalLong appliedCommitId() {
        return appliedCommitId;
    }

    /**
     * {@code sourceCommitId − appliedCommitId} clamped at {@code 0},
     * or {@code -1} when {@link #appliedCommitId()} is empty.
     */
    public long latencyMs() {
        return latencyMs;
    }

    @Override
    public String toString() {
        return "SyncLatency{sourceCommitId=" + sourceCommitId
                + ", appliedCommitId=" + (appliedCommitId.isPresent()
                        ? Long.toString(appliedCommitId.getAsLong()) : "<unknown>")
                + ", latencyMs=" + latencyMs + '}';
    }
}
