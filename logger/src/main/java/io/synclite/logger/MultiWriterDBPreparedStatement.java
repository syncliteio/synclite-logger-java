/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package io.synclite.logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.sqlite.SQLiteConnection;

public class MultiWriterDBPreparedStatement extends SyncLitePreparedStatement {

	private final PreparedStatement pstmt;
	public MultiWriterDBPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, sql);
		pstmt = getConn().getNativeDBConnection().prepareStatement(sql);
	}

	@Override
    final protected MultiWriterDBConnection getConn() {
        return ((MultiWriterDBConnection ) this.conn);
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
        long commitId = ((SyncLiteConnection ) this.conn).getCommitId();
        Object[] args = new Object[paramCount];
        for (int pos=0; pos < paramCount; pos++) {
            args[pos] = batch[batchPos + pos];
        }
        getCommandStager().log(commitId, sql, args);
    }
}
