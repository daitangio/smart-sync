package com.nttdata.gundam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.siforge.sm.SmartSync;

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
	public long populateDummyData(Connection conn, final int rows2pump)
			throws SQLException {
		Statement statement = conn.createStatement();
		
		statement.setQueryTimeout(30);  // set timeout to 30 sec.

		statement.executeUpdate("drop table if exists person");
		statement.executeUpdate("create table person (id integer, name string, surname string)");
		PreparedStatement ps=conn.prepareStatement("insert into person values(?, ?,?)");
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
