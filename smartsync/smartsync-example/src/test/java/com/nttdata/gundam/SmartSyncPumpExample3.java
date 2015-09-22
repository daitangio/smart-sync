package com.nttdata.gundam;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.siforge.sm.SmartSync;
import org.siforge.sm.SmartSyncBulk;
import org.siforge.sm.pump.SQLitePump;
import org.siforge.sm.pump.SmartSyncPump;
public class SmartSyncPumpExample3 {

	private Logger   logger=Logger.getLogger(getClass());
	public static void main(String[] args) throws ClassNotFoundException
	{
		Class.forName("org.sqlite.JDBC");
		(new SmartSyncPumpExample3()).playDemo();

	}

	private void playDemo() {
		logger.warn("How to populate an empty db with !SmartSync!");

		try
		{
			// create a database connection. Exmple of in memory
			
			SmartSyncPump b=new SQLitePump();
			b.setSource(getSrcDs());
			b.setDestination(getDestDs());
			
			//b.addTables("PERSON");
			
//			b.addTables(
//					"OTC_ASSET_CLASS_ANAG",
//					"OTC_ATTRIB_ANAG",
//					"OTC_ATTRIB_FAMILY_ASSOC",
//					"OTC_ATTRIB_FORMAT",
//					"OTC_ATTRIB_LIST_ANAG",
//					"OTC_BUSINESS_LINE_ANAG",
//					"OTC_CONFIRM_TYPE_ANAG",
//					"OTC_DBSMART_MATCH",
//					"OTC_DEAL",
//					"OTC_DEAL_ATTRIB_ASSOC",
//					"OTC_DEAL_NOTE_TEXT",
//					"OTC_DEAL_SECTION_ATTACH",
//					"OTC_DEAL_STATUS_ANAG",
//					"OTC_DEAL_STATUS_ASSOC",
//					"OTC_FAMILY",
//					"OTC_FORMULA_ANAG",
//					"OTC_FORMULA_DEP",
//					"OTC_LIST_ANAG",
//					"OTC_LIST_ELEM_ANAG",
//					"OTC_PRD",
//					"OTC_PRD_ATTRIB_ASSOC",
//					"OTC_PRD_BUS_LINE_DIST_ASSOC",
//					"OTC_PRD_BUS_LINE_INV_ASSOC",
//					"OTC_PRD_CLASS_ANAG",
//					"OTC_PRD_STATUS_ANAG",
//					"OTC_PRD_STATUS_ASSOC",
//					"OTC_PRD_TEXT_SECTION_ASSOC",
//					"OTC_PRD_TYPE",
//					"OTC_SECTION_ANAG",
//					"OTC_TEXT",
//					"OTC_TRADE_RECAP_ATTACH",
//					"OTC_TS_STATUS_ANAG",
//					"OTC_TS_STATUS_ASSOC"		
//					);
			
			logger.info("Db1 and db2 ready. Demo sync db1->db2");
			
			
			b.syncAll();
			logger.info("Ends here");
			
		}
		catch(Throwable e)
		{
			logger.fatal("ERROR",e);		
		}
		finally
		{

		}

		
	}

	protected DataSource getSrcDs() {
		return new  javax.sql.DataSource(){

			@Override
			public PrintWriter getLogWriter() throws SQLException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public int getLoginTimeout() throws SQLException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public java.util.logging.Logger getParentLogger()
					throws SQLFeatureNotSupportedException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Connection getConnection() throws SQLException {
				// jdbc:oracle:thin:@//<host>:<port>/<servicename>
				//jdbc:sqlite:./db1.sqlite
				return  DriverManager.getConnection("jdbc:sqlite:./db1.sqlite");
			}

			@Override
			public Connection getConnection(String username, String password)
					throws SQLException {
				// TODO Auto-generated method stub
				return null;
			}};
	}

	protected DataSource getDestDs() {
		return new  javax.sql.DataSource(){

			@Override
			public PrintWriter getLogWriter() throws SQLException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public int getLoginTimeout() throws SQLException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public java.util.logging.Logger getParentLogger()
					throws SQLFeatureNotSupportedException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Connection getConnection() throws SQLException {
				
				return  DriverManager.getConnection("jdbc:sqlite:./emptydb.sqlite");
			}

			@Override
			public Connection getConnection(String username, String password)
					throws SQLException {

				return null;
			}};
	}

	public long populateDummyData(Connection conn, final int rows2pump)
			throws SQLException {
		Statement statement = conn.createStatement();
		statement.setQueryTimeout(30);  // set timeout to 30 sec.

		statement.executeUpdate("drop table if exists person");
		statement.executeUpdate("create table person (id integer, name string)");
		PreparedStatement ps=conn.prepareStatement("insert into person values(?, ?)");
		// Speed up sqlite
		conn.setAutoCommit(false);
		for(int i=1; i<=rows2pump; ){
			ps.setInt(1, i++);
			ps.setString(2,
					(Math.random()>0.5?
					"Mark":"JJ"));
			ps.executeUpdate();
		}
		conn.commit();
		ResultSet rs = statement.executeQuery("select count(*) AS C from person");
		long personTableSize=-1;
		while(rs.next())
		{
			personTableSize = rs.getLong("C");
			logger.info(" Persons:"+personTableSize);		
			
		}
		return personTableSize;
		
	}
}
