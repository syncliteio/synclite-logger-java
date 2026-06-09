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
import java.util.Optional;

/**
 * Destination configuration consumed by
 * {@link SyncLite#initialize(io.synclite.DeviceType, java.nio.file.Path, String, DestinationOptions)}.
 *
 * <p>Field semantics mirror the Rust SDK's {@code DestinationOptions}:
 * {@code database} is required for {@link DstType#POSTGRES} and
 * {@link DstType#DUCKDB} and rejected for {@link DstType#SQLITE};
 * {@code schema} is required for Postgres and optional for DuckDB.
 *
 * <p>The {@link #connectionString()} accepts either the JDBC form or the
 * native form for every supported backend; both are equivalent:
 * <table>
 *   <caption>Accepted connection-string forms</caption>
 *   <tr><th>Backend</th><th>JDBC form</th><th>Native form</th></tr>
 *   <tr><td>{@link DstType#SQLITE SQLITE}</td>
 *       <td>{@code jdbc:sqlite:/path/to/file.db}</td>
 *       <td>{@code sqlite:///path/to/file.db}, {@code file:/path/to/file.db}, or a bare path</td></tr>
 *   <tr><td>{@link DstType#DUCKDB DUCKDB}</td>
 *       <td>{@code jdbc:duckdb:/path/to/file.duckdb}</td>
 *       <td>{@code duckdb:/path/to/file.duckdb} or a bare path</td></tr>
 *   <tr><td>{@link DstType#POSTGRES POSTGRES}</td>
 *       <td>{@code jdbc:postgresql://user:pw@host:5432/db}</td>
 *       <td>{@code postgresql://user:pw@host:5432/db} or libpq key/value</td></tr>
 * </table>
 * Query-string suffixes (e.g. {@code ?journal_mode=WAL}) are accepted
 * for SQLite/DuckDB and stripped during path extraction.
 */
public final class DestinationOptions {

    private final DstType dstType;
    private final String connectionString;
    private final String database;
    private final String schema;
    private final DstSyncMode syncMode;

    private DestinationOptions(Builder b) {
        this.dstType = Objects.requireNonNull(b.dstType, "dstType");
        this.connectionString = Objects.requireNonNull(b.connectionString, "connectionString");
        this.database = b.database;
        this.schema = b.schema;
        this.syncMode = b.syncMode != null ? b.syncMode : DstSyncMode.CONSOLIDATION;
        validate();
    }

    private void validate() {
        switch (dstType) {
            case SQLITE:
                if (database != null) {
                    throw new IllegalArgumentException("SQLITE destination must not specify database");
                }
                if (schema != null) {
                    throw new IllegalArgumentException("SQLITE destination must not specify schema");
                }
                break;
            case DUCKDB:
                if (database == null) {
                    throw new IllegalArgumentException("DUCKDB destination requires database");
                }
                break;
            case POSTGRES:
                if (database == null) {
                    throw new IllegalArgumentException("POSTGRES destination requires database");
                }
                if (schema == null) {
                    throw new IllegalArgumentException("POSTGRES destination requires schema");
                }
                break;
            default:
                throw new IllegalStateException("unknown DstType " + dstType);
        }
    }

    public DstType dstType() { return dstType; }
    public String connectionString() { return connectionString; }
    public Optional<String> database() { return Optional.ofNullable(database); }
    public Optional<String> schema() { return Optional.ofNullable(schema); }
    public DstSyncMode syncMode() { return syncMode; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private DstType dstType;
        private String connectionString;
        private String database;
        private String schema;
        private DstSyncMode syncMode;

        public Builder dstType(DstType v)            { this.dstType = v; return this; }
        public Builder connectionString(String v)    { this.connectionString = v; return this; }
        public Builder database(String v)            { this.database = v; return this; }
        public Builder schema(String v)              { this.schema = v; return this; }
        public Builder syncMode(DstSyncMode v)       { this.syncMode = v; return this; }

        public DestinationOptions build() { return new DestinationOptions(this); }
    }
}
