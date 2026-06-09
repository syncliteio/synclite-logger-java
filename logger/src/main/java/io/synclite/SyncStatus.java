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

import java.util.Objects;

/**
 * Snapshot of the consolidator's run state and last-heartbeat row.
 * Returned by {@link SyncLite#syncStatus(java.nio.file.Path)}.
 */
public final class SyncStatus {

    private final SyncState state;
    private final String status;
    private final String statusDescription;
    private final long lastHeartbeatTimeMs;

    SyncStatus(SyncState state, String status, String statusDescription, long lastHeartbeatTimeMs) {
        this.state = Objects.requireNonNull(state, "state");
        this.status = status == null ? "" : status;
        this.statusDescription = statusDescription == null ? "" : statusDescription;
        this.lastHeartbeatTimeMs = lastHeartbeatTimeMs;
    }

    public SyncState state() {
        return state;
    }

    /**
     * Raw {@code status} string from the consolidator's {@code device_status}
     * row (e.g. {@code "SYNCING"}). Empty until the first heartbeat lands.
     */
    public String status() {
        return status;
    }

    public String statusDescription() {
        return statusDescription;
    }

    /** {@code device_status.last_heartbeat_time} (epoch ms). {@code 0} if absent. */
    public long lastHeartbeatTimeMs() {
        return lastHeartbeatTimeMs;
    }

    @Override
    public String toString() {
        return "SyncStatus{state=" + state
                + ", status='" + status + '\''
                + ", statusDescription='" + statusDescription + '\''
                + ", lastHeartbeatTimeMs=" + lastHeartbeatTimeMs + '}';
    }
}
