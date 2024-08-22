package io.synclite.logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.sqlite.SQLiteConnection;

public class MultiWriterDBAppenderPreparedStatement extends SyncLiteAppenderPreparedStatement {

	private final PreparedStatement pstmt;
	public MultiWriterDBAppenderPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, sql);
		pstmt = getConn().getNativeDBConnection().prepareStatement(sql);
	}

	@Override
    final protected MultiWriterDBAppenderConnection getConn() {
        return ((MultiWriterDBAppenderConnection ) this.conn);
    }
        
    @Override
    final protected boolean pStmtExecute() throws SQLException {
    	if (tableNameInDDL != null) {
    		throw new SQLException("DDL statements not permitted with PreparedStatement");
    	}

    	//If args were set but batch was not added, then add them as a batch
    	boolean res = false;
    	if ((batchQueryCount == 0) && (batch != null) && (paramCount > 0)) {
    		for (int pos=0; pos < paramCount; pos++) {
    			Object o = batch[batchPos + pos];
    			pstmt.setObject(pos+1, o);
    		}
            pstmt.addBatch();
            pstmt.executeBatch();            
            
            res = false;
    	} else {
    		res = pstmt.execute();
    	}
    	
    	return res;
    }
    
    @Override
    final protected void pStmtAddBatch() throws SQLException {
		for (int pos=0; pos < paramCount; pos++) {
			Object o = batch[batchPos + pos];
			pstmt.setObject(pos+1, o);
		}
        pstmt.addBatch();
        ++batchQueryCount;
    }
    
    @Override
    final protected int[] pStmtExecuteBatch() throws SQLException {
    	if (tableNameInDDL != null) {
    		throw new SQLException("DDL statements not permitted with PreparedStatement");
    	}
    
    	int[] res = pstmt.executeBatch();
    	
    	return res;
    }

    
    @Override
    final public void close() throws SQLException {
        super.close();
        if (this.pstmt != null) {
        	pstmt.close();
        }
    }

    private SQLStager getCommandStager() {
    	return getConn().getCommandStager();
    }
    
    @Override
    protected final void log() throws SQLException {
		long commitId = ((SyncLiteAppenderConnection ) this.conn).getCommitId();
		Object[] args = new Object[paramCount];
		for (int pos=0; pos < paramCount; pos++) {
			args[pos] = batch[batchPos + pos];
		}
        getCommandStager().log(commitId, sql, args);
    }

    @Override
	protected final void log(Object[] args) throws SQLException {
		long commitId = ((SyncLiteAppenderConnection ) this.conn).getCommitId();
		if (batchQueryCount == 0) {
	        getCommandStager().log(commitId, this.sql, args);
		} else if (batchQueryCount == 1){
	        getCommandStager().log(commitId, this.sql, args);
		} else {
	        getCommandStager().log(commitId, null, args);
		}
	}
}
