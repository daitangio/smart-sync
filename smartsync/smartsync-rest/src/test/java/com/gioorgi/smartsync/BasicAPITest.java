package com.gioorgi.smartsync;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jca.work.WorkManagerTaskExecutor;
import org.springframework.web.client.RestTemplate;

import com.gioorgi.smartsync.rest.SyncReport;

public class BasicAPITest {
	Logger logger=Logger.getLogger(getClass());
	
	final static String endpoint="http://localhost:4000/";
	final static String workingDir=System.getProperty("user.home")+"/synctest/";
	RestTemplate rt= new RestTemplate();
	
	@BeforeClass
	public static void buildSympleDB(){
		//BasicConfigurator.configure();
		Logger.getLogger(BasicAPITest.class).info("Building base test db onto:"+workingDir);
		Connection connection = null;
		try
		{
			// create a database connection. Exmple of in memory
			connection = DriverManager.getConnection("jdbc:sqlite:"+workingDir+"./testUnitDB.sqlite");
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
	
			statement.executeUpdate("drop table if exists person");
			statement.executeUpdate("create table person (id integer, name string)");
			statement.executeUpdate("insert into person values(1, 'leo')");
			statement.executeUpdate("insert into person values(2, 'yui')");
			ResultSet rs = statement.executeQuery("select * from person");
			while(rs.next())
			{
				// read the result set
				System.out.println("name = " + rs.getString("name"));
				System.out.println("id = " + rs.getInt("id"));
			}
			Logger.getLogger(BasicAPITest.class).info("Test database ok");
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
	
	
	
	
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	@Test
	public void testConnect() {
		SyncReport sr=rt.getForObject(endpoint+"smartsync-rest/rpc/sys/status", 
				SyncReport.class);
		assertEquals(1.0f,sr.getVersion(),0);
		System.out.println(""+sr);
	}
	
	//@Test
	public void testSimpleCopyFromSqlLite2SqlLite(){
		// public SyncReport copy2Sqlite(String jdbcSrc, String username, String password, List<String> tableList)
		SyncReport sr=rt.getForObject(endpoint+"smartsync-rest/rpc/sys/copy2sqlite", 
				SyncReport.class,
				"jdbc:sqlite:"+workingDir+"./testUnitDBCopied.sqlite",
				"","",
				"person"
				);
	}

}
