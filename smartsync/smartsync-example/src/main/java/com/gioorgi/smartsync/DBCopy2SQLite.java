package com.gioorgi.smartsync;

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
import javax.swing.text.DateFormatter;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.ISO8601DateFormat;
import org.siforge.sm.SmartSync;
import org.siforge.sm.SmartSyncBulk;
import org.siforge.sm.pump.SQLitePump;
import org.siforge.sm.pump.SmartSyncPump;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
public class DBCopy2SQLite extends DataSourceSillyProvider {

	private Logger   logger=Logger.getLogger(getClass());
	public static void main(String[] args) throws ClassNotFoundException
	{
		Class.forName("org.sqlite.JDBC");
		String srcJdbc=args[0];
		String username=args[1], pw=args[2];
		String flist=args[3];
		(new DBCopy2SQLite(srcJdbc, username,pw,flist)).copy();

	}

	String extractQuery;
	List<String> tableList=null;
	public DBCopy2SQLite(String srcJdbc, String username, String  pw, String flist){
		super(srcJdbc, username,  pw, new SQLitePump());
		this.extractQuery=flist;
	}

	public DBCopy2SQLite(String srcJdbc, String username, String  pw, List<String> tableList){
		super(srcJdbc, username,  pw, new SQLitePump());
		this.extractQuery=null;
		this.tableList=tableList;
	}
	
	
	public void copy() {
		logger.warn("How to populate an empty db with !SmartSync!");

		try
		{
			// create a database connection. Exmple of in memory

			SmartSyncPump b=new SQLitePump();
			b.setSource(getSrcDs());
			b.setDestination(getDestDs());

			//b.addTables("PERSON");
			NamedParameterJdbcTemplate t=new NamedParameterJdbcTemplate(getSrcDs());
			// public <T> List<T> queryForList(String sql, Map<String, ?> paramMap, Class<T> elementType)
			final List<String> tables;
			if(tableList==null){ 
				tables= t.queryForList(this.extractQuery, new TreeMap<String, Object>(), String.class);
			}else{
				tables=tableList;
			}
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
