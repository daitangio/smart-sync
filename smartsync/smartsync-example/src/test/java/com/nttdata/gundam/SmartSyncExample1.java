package com.nttdata.gundam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.siforge.sm.SmartSync;
public class SmartSyncExample1 {

	private Logger   logger=Logger.getLogger(getClass());
	public static void main(String[] args) throws ClassNotFoundException
	{
		//BasicConfigurator.configure();
		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");
		(new SmartSyncExample1()).playDemo();

	}

	private void playDemo() {
		Connection connection = null,db2;
		try
		{
			// create a database connection. Exmple of in memory
			connection = DriverManager.getConnection("jdbc:sqlite:./db1.sqlite");
			db2=DriverManager.getConnection("jdbc:sqlite:./db2.sqlite");
			long expectedResult=populateDummyData(connection,21439*3);
			expectedResult+=populateDummyData(db2,30);
			logger.info("Db1 and db2 ready. Demo sync db1->db2");
			logger.info("Final size expected:"+expectedResult);
			SmartSync s1=new SmartSync("PERSON",connection,db2);
			s1.syncAll();
			ResultSet rs=db2.prepareStatement("select count(*) AS C from person").executeQuery();
			rs.next();
			logger.info("DEST DB SIZE:"+rs.getObject("C"));
			if(expectedResult==rs.getLong("C") ){
				logger.info("Smart Sync worked");
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
