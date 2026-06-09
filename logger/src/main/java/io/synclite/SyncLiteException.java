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
 * Unchecked exception raised by the native runtime layer. Propagated
 * from JNI via {@code env.throw_new("io/synclite/runtime/SyncLiteException", ...)}.
 */
public class SyncLiteException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public SyncLiteException(String message) { super(message); }
    public SyncLiteException(String message, Throwable cause) { super(message, cause); }
}
