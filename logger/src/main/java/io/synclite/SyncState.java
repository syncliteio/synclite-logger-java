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
 * Run state of a device's sync pipeline. Mirrors
 * {@code synclite::SyncState} in the Rust crate. Ordinals are part
 * of the JNI contract — do not reorder.
 */
public enum SyncState {
    /** Device has never been initialized (no {@code .synclite} metadata). */
    NOT_INITIALIZED,
    /** {@link SyncLite#pauseSync(java.nio.file.Path)} was called and not yet resumed. */
    PAUSED,
    /** Default — consolidator is processing segments as they arrive. */
    RUNNING
}
