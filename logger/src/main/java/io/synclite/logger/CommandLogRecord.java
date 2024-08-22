package io.synclite.logger;

class CommandLogRecord {
    public String dbPath;
    public long commitId;
    public String sql;
    public Object[] args;

    CommandLogRecord(long commitId, String sql, Object[] args) {
        this.commitId = commitId;
        this.sql = sql;
        this.args = args;
    }
};


