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
 * Synchronization mode applied by the consolidator when draining
 * segments to the destination.
 */
public enum DstSyncMode {
    CONSOLIDATION,
    REPLICATION;
}
