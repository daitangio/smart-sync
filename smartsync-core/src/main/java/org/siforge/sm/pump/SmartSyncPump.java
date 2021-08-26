package org.siforge.sm.pump;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.siforge.sm.MetaSupport;
import org.siforge.sm.SmartSyncBulk;
import org.siforge.sm.SyncException;

/**
 * A class which "suck" data structure from a database and replicate it in the target.
 * You need a specific adapter subclass for the destination database
 * and it uses 
 * @author Giorgig
 *
 */
public abstract class SmartSyncPump extends MetaSupport {
	protected Logger logger = Logger.getLogger(getClass());

	private DataSource source, destination;
	private List<String> relations2Sync = new LinkedList<String>();
	private int threads = 2;
	abstract void createTable(String tableName, Connection destConnection, ResultSetMetaData metaData, int... jdbcTypes ) throws SQLException;

	/** Given a file path, give me an happy jdbc string
	 * 
	 * @param filename
	 * @return
	 */
	public abstract String jdbcString(String filename) ;

	/* Simple, not parallel sync
	 */
	public void syncAll(){
		syncAll(false);
	}
	/**
	 * 
	 * @param parallel when true try parallel mode. It seems bugged, avoid it for the meantime
	 */
	public void syncAll(boolean parallel){
		try{
			logger.trace("Ensuring all tables are in dest...");
			Connection sc=source.getConnection();
			Connection ensurer=destination.getConnection();
			for (String table : relations2Sync) {
				try {
					ensurer.prepareStatement("SELECT * FROM "+table).executeQuery();
					logger.info(table+" EXIST...OK");
				}catch(SQLException sqe){
					logger.info("Creating dest table:"+table);
					PreparedStatement limited=sc.prepareStatement("SELECT * FROM "+table);
					limited.setFetchSize(1);
					limited.setMaxRows(1); 
					ResultSet rs=limited.executeQuery();
					createTable(table, ensurer,rs.getMetaData(),this.getTypes(rs.getMetaData()));
				}
			}
			sc.close();
			ensurer.close();
			SmartSyncBulk bulk=new SmartSyncBulk();
			bulk.setSource(source);
			bulk.setDestination(destination);
			bulk.addTables(relations2Sync);
			bulk.setThreads(8);
			if(parallel){
				logger.debug("Running in parallel mode");
				bulk.syncAllParallel();
			}else {
				logger.debug("Running in linear mode");
				bulk.syncAll();
			}
		} catch (SQLException e) {
			throw new SyncException(e);
		}finally {

		}
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public void setSource(DataSource source) {
		this.source = source;
	}

	public void setDestination(DataSource destination) {
		this.destination = destination;
	}

	public void setRelations2Sync(List<String> relations2Sync) {
		this.relations2Sync = relations2Sync;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}
	
	
	public void addTables(String ...t){
		relations2Sync.addAll(Arrays.asList(t));
	}
	
	public void addTables(List<String> relations2Sync2) {
		this.relations2Sync.addAll(relations2Sync2);	
	}
}
