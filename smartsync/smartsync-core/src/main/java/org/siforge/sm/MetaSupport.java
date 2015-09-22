package org.siforge.sm;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class MetaSupport {

	/** Log dell'istanza */
	protected Logger logger=Logger.getLogger(getClass());
	protected int srcColType[];
	protected int destColType[];

	protected int findType(int pos) {
		return this.srcColType[pos-1];
	
	}

	/**
	 * @param type
	 * @return
	 */
	private String typeToString(int type) {
		switch(type) {
		case java.sql.Types.ARRAY:return "ARRAY";
		case java.sql.Types.BIGINT: return "BIGINT";
		case java.sql.Types.BINARY: return "BINARY";
		case java.sql.Types.BIT: return "BIT";
		case java.sql.Types.BLOB: return "BLOB";
		case java.sql.Types.CHAR: return "CHAR";
		case java.sql.Types.CLOB: return "CLOB";
		case java.sql.Types.DATE: return "DATE";
		case java.sql.Types.DECIMAL: return "DECIMAL";
		case java.sql.Types.DISTINCT: return "DISTINCT";
		case java.sql.Types.DOUBLE: return "DOUBLE";
		case java.sql.Types.FLOAT: return "FLOAT";
		case java.sql.Types.INTEGER: return "INTEGER";
		case java.sql.Types.JAVA_OBJECT: return "JAVA_OBJECT";
		case java.sql.Types.LONGVARBINARY: return "LONGVARBINARY";
		case java.sql.Types.LONGVARCHAR: return "LONGVARCHAR";
		case java.sql.Types.NULL: return "NULL";
		case java.sql.Types.NUMERIC: return "NUMERIC";
		case java.sql.Types.OTHER: return "OTHER";
		case java.sql.Types.REAL: return "REAL";
		case java.sql.Types.REF: return "REF";
		case java.sql.Types.SMALLINT: return "SMALLINT";
		case java.sql.Types.STRUCT: return "STRUCT";
		case java.sql.Types.TIME: return "TIME";
		case java.sql.Types.TIMESTAMP: return "TIMESTAMP";
		case java.sql.Types.TINYINT: return "TINYINT";
		case java.sql.Types.VARBINARY: return "VARBINARY";
		case java.sql.Types.VARCHAR : return "VARCHAR";
		default:
			return"UNKNOWN_SQL_TYPE?";
		} // switch
	}

	/** Questa funzione ritorna i tipi delle colonne  del ResultSetMetaData
	 * fornito.
	 * Stampa nel log i tipi, in formato comprensibile
	 */
	protected int [] getTypes(ResultSetMetaData md) throws SQLException {
		int colType[];
		int cols=md.getColumnCount();
	
		colType=new int[cols];
		int t=0;
		String msg="  Types:";
		while(t<cols){
			// Rec: le pos partono da 1, gli array da zero!
			colType[t]=md.getColumnType(t+1);
			msg+=typeToString(colType[t]) + ":";
			t++;
		}
	
		logger.debug(msg);
		return colType;
	}

}
