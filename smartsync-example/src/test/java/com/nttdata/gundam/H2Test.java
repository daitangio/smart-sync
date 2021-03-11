package com.nttdata.gundam;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.siforge.sm.SyncException;
import org.siforge.sm.pump.H2Pump;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.gioorgi.smartsync.DataSourceSillyProvider;

public class H2Test extends DataSourceSillyProvider{
	private Logger   logger=Logger.getLogger(getClass());
	public static void main(String[] args) throws ClassNotFoundException 
	{
		Class.forName("org.h2.Driver");
//		String srcJdbc=args[0];
//		String username=args[1], pw=args[2];
//		String flist=args[3];
//		(new H2Test(srcJdbc, username,pw)).test();
		(new H2Test("jdbc:h2:./test.h2.db", "","")).test();
		
	}

	public H2Test(String srcJdbc, String username, String pw) {
		super(srcJdbc, username, pw, new H2Pump());
	}

	private void test() {
		try {
			super.getSrcDs().getConnection().close();
			logger.info("H2 Connection works");
			NamedParameterJdbcTemplate t=new NamedParameterJdbcTemplate(getSrcDs());
			Connection c=super.getSrcDs().getConnection();
			c.prepareStatement("drop table test if exists").execute();
			c.prepareStatement("create table test (s varchar2(10), i integer, r real )").execute();
			c.close();
			logger.info("test table creation ok");
			logger.info("Your H2 installation seems good");
		} catch (SQLException e) {
			throw new SyncException(e);
		}
		
	}
	

}
