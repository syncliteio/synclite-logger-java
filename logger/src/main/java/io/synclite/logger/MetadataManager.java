package io.synclite.logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

final class MetadataManager {
    private Path metadataFilePath;
    private Connection metadataTableConn = null;
    private static final String insertMetadataSql = "INSERT INTO metadata VALUES (?,?)";
    private PreparedStatement insertMetadataStmt = null;
    private static final String updateMetadataSql = "UPDATE metadata SET value = ? WHERE key = ?";
    private PreparedStatement updateMetadataStmt = null;
    private static final String deleteMetadataSql = "DELETE FROM metadata WHERE key = ?";
    private PreparedStatement deleteMetadataStmt = null;
    private static final String selectMetadataSql = "SELECT value FROM metadata WHERE key = ?";
    private PreparedStatement selectMetadataStmt = null;

    MetadataManager(Path metadataFilePath) throws SQLException {
        this.metadataFilePath = metadataFilePath;
        try {
	        metadataTableConn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath);
	        initializeMetadataTable();
	        insertMetadataStmt = metadataTableConn.prepareStatement(insertMetadataSql);
	        updateMetadataStmt = metadataTableConn.prepareStatement(updateMetadataSql);
	        deleteMetadataStmt = metadataTableConn.prepareStatement(deleteMetadataSql);
	        selectMetadataStmt = metadataTableConn.prepareStatement(selectMetadataSql);
        } catch (SQLException e) {
        	throw new SQLException("Failed to open/write into the metadata file at path : " + metadataFilePath, e);
        }
    }

    final Path getMetadataFilePath() {
        return this.metadataFilePath;
    }

    private void initializeMetadataTable() throws SQLException {
        try (Statement stmt = metadataTableConn.createStatement()) {
        	stmt.execute("pragma page_size=512");
            stmt.execute("create table if not exists metadata(key text, value text);");
        } catch (SQLException e) {
        	throw new SQLException("Failed to initialize metadata table in the metadata file : " + metadataFilePath, e);
        }
    }

    void close() throws SQLException {
    	if (metadataTableConn != null) {
    		try {
    			metadataTableConn.close();
    		} catch (SQLException e) {
    			//Suppress
    		}
			metadataTableConn = null;
    	}
    }  

    final synchronized void replaceProperty(String key, Object value) throws SQLException {
        deleteMetadataStmt.setString(1, key);
        insertMetadataStmt.setString(1, key);
        insertMetadataStmt.setObject(2, value);
        metadataTableConn.setAutoCommit(false);
        deleteMetadataStmt.execute();
        insertMetadataStmt.execute();
        metadataTableConn.commit();
        metadataTableConn.setAutoCommit(true);
    }

    final synchronized void insertProperty(String key, Object value) throws SQLException {
    	insertMetadataStmt.setString(1, key);
    	insertMetadataStmt.setObject(2, value);
    	insertMetadataStmt.execute();
    }

    final synchronized void updateProperty(String key, Object value) throws SQLException {
    	updateMetadataStmt.setString(2, key);
    	updateMetadataStmt.setObject(1, value);
    	updateMetadataStmt.execute();
    }

    final String getStringProperty(String key) throws SQLException {
        selectMetadataStmt.setString(1, key);
        try (ResultSet rs = selectMetadataStmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
        };
        return null;
    }

    final Long getLongProperty(String key) throws SQLException {
        selectMetadataStmt.setString(1, key);
        try (ResultSet rs = selectMetadataStmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        };
        return null;
    }

	public void backupMetadataFile(Path metadataFileBackupPath) throws SQLException {
		try (Statement stmt = metadataTableConn.createStatement()) {
			stmt.executeUpdate("BACKUP TO '" + metadataFileBackupPath + "'");
		}
	}
	
}
