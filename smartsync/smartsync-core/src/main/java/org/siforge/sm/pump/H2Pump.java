package org.siforge.sm.pump;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.siforge.sm.SyncException;

/**
 * Hypersonic2 SQL could be ideal for pure-java implementations. 
 * @author Giorgig
 *
 */
public class H2Pump extends SmartSyncPump {

	@Override
	void createTable(String tableName, Connection destConnection,
			ResultSetMetaData metaData, int... jdbcTypes) 
					throws SQLException {
		String sql="Create table "+tableName+" ( ";
		for(int i=1; i<=metaData.getColumnCount(); i++){
			sql+=" "+metaData.getColumnName(i);

			int type=metaData.getColumnType(i);
			switch(type){
				case java.sql.Types.DATE:
					sql+= " date";
					break;
				case java.sql.Types.INTEGER:
					sql+=" int";
					break;
				case java.sql.Types.NUMERIC:
				case java.sql.Types.DECIMAL:
				case java.sql.Types.FLOAT:					
				case java.sql.Types.DOUBLE:
					sql +=" real";
					break;
				/** TODO  FIXME TEMP */
				case java.sql.Types.BINARY:
				case java.sql.Types.CLOB:
				case java.sql.Types.BLOB:
					sql+=" varchar2("+Integer.MAX_VALUE+")";
					break;
					
				case java.sql.Types.VARCHAR:
				default:			
					logger.warn("Type:"+metaData.getColumnTypeName(i));
					sql+=" varchar2("+metaData.getPrecision(i)+")";
					break;

			}

			if(i != metaData.getColumnCount()){
				sql+=",";
			}
		}
		sql+=")";
		logger.trace("Generic...."+ sql);
		destConnection.prepareStatement(sql).executeUpdate();
	}

	@Override
	public
	String jdbcString(String filename) {		
		return "jdbc:h2:"+filename+".h2";
	}



}
