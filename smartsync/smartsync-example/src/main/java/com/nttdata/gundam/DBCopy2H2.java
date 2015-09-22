package com.nttdata.gundam;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.DataSource;
import javax.swing.text.DateFormatter;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.ISO8601DateFormat;
import org.siforge.sm.SmartSync;
import org.siforge.sm.SmartSyncBulk;
import org.siforge.sm.pump.H2Pump;
import org.siforge.sm.pump.SQLitePump;
import org.siforge.sm.pump.SmartSyncPump;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.h2.jdbcx.JdbcConnectionPool;
public class DBCopy2H2 extends DataSourceSillyProvider {

	private Logger   logger=Logger.getLogger(getClass());
	SmartSyncPump b;
	public static void main(String[] args) throws ClassNotFoundException
	{
		Class.forName("org.sqlite.JDBC");
		String srcJdbc=args[0];
		String username=args[1], pw=args[2];
		String flist=args[3];
		(new DBCopy2H2(srcJdbc, username,pw,flist)).copy();

	}

	String extractQuery;
	public DBCopy2H2(String srcJdbc, String username, String  pw, String flist){
		super(srcJdbc, username,  pw, (new H2Pump()));
		b= new H2Pump();
		this.extractQuery=flist;
	}

	
	@Override
	protected DataSource getDestDs() {
		
		return JdbcConnectionPool.create(super.getDstJdbc(),"sa","sa");
	}


	private void copy() {
		/** GG CONSIDER FOR DEST SOMEGHINT LIKE
		 * import org.h2.jdbcx.JdbcConnectionPool;
			JdbcConnectionPool cp = JdbcConnectionPool.
			    create("jdbc:h2:~/test", "sa", "sa");
			Connection conn = cp.getConnection();
			conn.close(); cp.dispose();
			
			
			java -cp 'C:\Users\giorgig\.m2\repository\com\h2database\h2\1.4.188\h2-1.4.188.jar' org.h2.tools.Shell -url jdbc:h2:./dump_8-set-2015.db

		 */
		logger.warn("How to populate an empty db with !SmartSync!");

		try
		{
			// create a database connection. 			
			b.setSource(getSrcDs());
			b.setDestination(getDestDs());

			//b.addTables("PERSON");
			NamedParameterJdbcTemplate t=new NamedParameterJdbcTemplate(getSrcDs());
			// public <T> List<T> queryForList(String sql, Map<String, ?> paramMap, Class<T> elementType)
			List<String> tables= t.queryForList(this.extractQuery, new TreeMap<String, Object>(), String.class);
			
			b.addTables(tables);

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
