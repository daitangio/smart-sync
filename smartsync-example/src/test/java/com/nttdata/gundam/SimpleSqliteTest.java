package com.nttdata.gundam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import javax.sql.DataSource;

import org.junit.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.siforge.sm.SmartSync;
import org.siforge.sm.SmartSyncBulk;
import org.siforge.sm.pump.SQLitePump;
import org.siforge.sm.pump.SmartSyncPump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;

import junit.framework.TestCase;
// import org.junit.Test;
public class SimpleSqliteTest /*extends TestCase*/{
	private Logger   logger=Logger.getLogger(getClass());

	@Ignore
	@Test
	public void testPersonCopy() throws ClassNotFoundException
	{
		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");

		Connection connection = null;
		try
		{
			// create a database connection. Exmple of in memory
			connection = DriverManager.getConnection("jdbc:sqlite::memory:");
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.

			statement.executeUpdate("drop table if exists person");
			statement.executeUpdate("create table person (id integer, name string)");
			statement.executeUpdate("insert into person values(1, 'Anna Oxa')");
			statement.executeUpdate("insert into person values(2, 'Silvio Morandi')");
			ResultSet rs = statement.executeQuery("select * from person");
			while(rs.next())
			{
				// read the result set
				System.out.println("name = " + rs.getString("name"));
				System.out.println("id = " + rs.getInt("id"));
			}
		}
		catch(SQLException e)
		{
			// if the error message is "out of memory", 
			// it probably means no database file is found
			System.err.println(e.getMessage());
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

	@Ignore
	@Test
	public void playDemo() {
		Connection db1source=null , db2dest=null;
		try
		{
			db1source = DriverManager.getConnection("jdbc:sqlite:./db1.sqlite");
			db2dest=DriverManager.getConnection("jdbc:sqlite:./db2.sqlite");
			// Push different data on db1 and db2. db1 is bigger
			long expectedResult=populateDummyData(db1source,21439*3);
			expectedResult+=populateDummyData(db2dest,30);
			logger.info("Db1 and db2 ready. Demo sync db1->db2");
			logger.info("Final size expected:"+expectedResult);
			SmartSync s1=new SmartSync("PERSON",db1source,db2dest);
			s1.syncAll();
			ResultSet rs=db2dest.prepareStatement("select count(*) AS C from person").executeQuery();
			rs.next();
			logger.info("DEST DB SIZE:"+rs.getObject("C"));
			assert expectedResult==rs.getLong("C") ;			
		}
		catch(SQLException e)
		{
			// if the error message is "out of memory", 
			// it probably means no database file is found
			System.err.println(e.getMessage());
		}
		finally
		{
			try
			{
				if(db1source != null)
					db1source.close();

				if(db2dest != null)
					db2dest.close();
			}
			catch(SQLException e)
			{
				// connection close failed.
				System.err.println(e);
			}
		}

		
	}

	
	@Test
	public void testBulkBug() throws SQLException{
		Connection connection = DriverManager.getConnection("jdbc:sqlite:./db-src.sqlite");
		File destFile=new File("./db-dest.sqlite");
		if (destFile.exists()){
			destFile.delete();
		}
		Connection db2=DriverManager.getConnection("jdbc:sqlite:./db-dest.sqlite");
		long expectedResult=0;
		List<String> tableNames=Arrays.asList("person","pet","pup","croc","cat","fish", "dog");

		for (String tb : tableNames) {
			expectedResult+=populateDummyData(connection,40000,tb);				
		}
		connection.close();
		db2.close();
		SmartSyncPump b=new SQLitePump();
		b.setSource(getSrcDs());
		b.setDestination(getDestDs());
		b.addTables(tableNames);				
		
		logger.info("Db1 and db2 ready. Demo sync db1->db2");
		logger.info("Final size expected:"+expectedResult);
		
		b.syncAll(true);
		long  result=0;
		db2=getDestDs().getConnection();
		for (String tb : tableNames) {
			ResultSet rs=db2.prepareStatement("select count(*) AS C from "+tb).executeQuery();
			rs.next();
			result +=rs.getLong("C");
			logger.info(tb+"#"+result);
		}
		logger.info("DEST DB SIZE:"+result);
		if(expectedResult==result ){
			logger.info("Smart Sync Bulk worked");
		}else{
			logger.fatal("Expected:"+expectedResult+" Got:"+result);
			fail("Expected:"+expectedResult+" Got:"+result);
		}
	}

	/**
	 * Test to show how to convert raw numbers from date object
	 * in the mssql ->sqlite use case
	 */
	@Ignore
	@Test 
	public void mssqlserver2sqlite_date_conv_check(){
		//should match 2020-10-12 22:58:35
		assertEquals("Mon Oct 12 22:58:35 CEST 2020", ""+(new java.util.Date(1602536315687l)));	
		
		assertEquals("Thu Oct 29 23:28:38 CET 2020", ""+(new java.util.Date(1604010518700l)));
		
	}

	@Ignore
	@Test
	public void dataMaskDemo() throws SQLException{
		Connection db1source=null , db2dest=null;

		db1source = DriverManager.getConnection("jdbc:sqlite:./db1.sqlite");
		db2dest=DriverManager.getConnection("jdbc:sqlite:./db2.sqlite");
		// Push different data on db1 and db2. db1 is bigger
		long expectedResult=populateDummyData(db1source,50);		
		populateDummyData(db2dest,0); // Empty database
		logger.info("Db1 and db2 ready. Demo sync db1->db2");
		logger.info("Final size expected:"+expectedResult);
		SmartSync s1=new SmartSync("PERSON",db1source,db2dest);
		s1.mask("NAME","***");
		s1.syncAll();
		ResultSet rs=db2dest.prepareStatement("select (name || ' ' || surname) AS MASKED from person").executeQuery();
		rs.next();
		String masked=rs.getString("MASKED");
		db1source.close();
		db2dest.close();

		if(Arrays.asList(TEST_NAMES).contains(masked)){
			throw new RuntimeException("Data not masked:"+masked);
		}
	}

	final static String TEST_NAMES[]=new String[] { 
		"Silvio Morandi", "Anna Oxa", "Mark Zuckberg", 
		"Mark Twain", "Laura Pausini", "Emma Marrone","Alice Merton",
		"Arisa Arona",
		"Enzo Jannacci"
	 };
	final Random randomGen= new Random(23);
	public long populateDummyData(Connection conn, final int rows2pump) throws SQLException{
		return populateDummyData(conn, rows2pump,"person");
	}
	public long populateDummyData(Connection conn, final int rows2pump, String tablename)
			throws SQLException {
		logger.info("Populating "+tablename);
		Statement statement = conn.createStatement();
		
		statement.setQueryTimeout(30);  // set timeout to 30 sec.

		statement.executeUpdate("drop table if exists "+tablename);
		statement.executeUpdate("create table "+tablename+" (id integer, name string, surname string)");
		PreparedStatement ps=conn.prepareStatement("insert into "+tablename+" values(?, ?,?)");
		// Speed up sqlite
		conn.setAutoCommit(false);
		for(int i=1; i<=rows2pump; ){
			ps.setInt(1, i++);

			String dummyData=TEST_NAMES[randomGen.nextInt(TEST_NAMES.length)];
			String name=dummyData.split(" ")[0];
			String surname=dummyData.split(" ")[1];

			ps.setString(2,name);
			ps.setString(3,surname);
			ps.executeUpdate();
		}

		conn.commit();
		ResultSet rs = statement.executeQuery("select count(*) AS C from "+tablename);
		long personTableSize=-1;
		while(rs.next())
		{
			personTableSize = rs.getLong("C");
			logger.info(" "+tablename+"#"+personTableSize);		
			
		}
		return personTableSize;
		
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
				
				return  DriverManager.getConnection("jdbc:sqlite:./db-src.sqlite");
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
				
				return  DriverManager.getConnection("jdbc:sqlite:./db-dest.sqlite");
			}

			@Override
			public Connection getConnection(String username, String password)
					throws SQLException {
				// TODO Auto-generated method stub
				return null;
			}};
	}

}
