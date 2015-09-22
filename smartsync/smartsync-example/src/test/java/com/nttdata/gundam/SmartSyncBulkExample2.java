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
public class SmartSyncBulkExample2 {

	private Logger   logger=Logger.getLogger(getClass());
	public static void main(String[] args) throws ClassNotFoundException
	{
		Class.forName("org.sqlite.JDBC");
		(new SmartSyncBulkExample2()).playDemo();

	}

	private void playDemo() {
		logger.warn("With SQLITE, Bulk sync performance could be poor. Try out a ORACLE!");
		Connection connection = null,db2;
		try
		{
			// create a database connection. Exmple of in memory
			connection = DriverManager.getConnection("jdbc:sqlite:./db1.sqlite");
			db2=DriverManager.getConnection("jdbc:sqlite:./db2.sqlite");
			long expectedResult=populateDummyData(connection,21439*3);

			SmartSyncBulk b=new SmartSyncBulk();
			b.setSource(getSrcDs());
			b.setDestination(getDestDs());
			
			b.addTables("PERSON","PERSON","PERSON");
			
			expectedResult*=b.getRelations2SyncSize();

			expectedResult+=populateDummyData(db2,30);
			connection.close();
			db2.close();
			logger.info("Db1 and db2 ready. Demo sync db1->db2");
			logger.info("Final size expected:"+expectedResult);
			
			b.syncAll();
			ResultSet rs=getDestDs().getConnection().prepareStatement("select count(*) AS C from person").executeQuery();
			rs.next();
			logger.info("DEST DB SIZE:"+rs.getObject("C"));
			if(expectedResult==rs.getLong("C") ){
				logger.info("Smart Sync Bulk worked");
			}else{
				logger.fatal("Expected:"+expectedResult+" Got:"+rs.getLong("C"));
			}
			
		}
		catch(Throwable e)
		{
			logger.fatal("ERROR",e);		
		}
		finally
		{
			try
			{
				if(connection != null)
					connection.close();
			}
			catch(SQLException e)
			{
				// connection close failed.
				System.err.println(e);
			}
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
				
				return  DriverManager.getConnection("jdbc:sqlite:./db2.sqlite");
			}

			@Override
			public Connection getConnection(String username, String password)
					throws SQLException {
				// TODO Auto-generated method stub
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
