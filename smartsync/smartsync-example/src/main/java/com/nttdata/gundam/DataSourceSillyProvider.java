package com.nttdata.gundam;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.log4j.helpers.ISO8601DateFormat;
import org.siforge.sm.pump.SmartSyncPump;

public class DataSourceSillyProvider {

	private String srcJdbc;
	private String username;
	private String pw;
	private String dstJdbc;

	public DataSourceSillyProvider() {
		super();
	}
	
	public DataSourceSillyProvider(String srcJdbc, String username, String  pw, SmartSyncPump pump){
		this.srcJdbc=srcJdbc; this.username=username; this.pw=pw; 
		dstJdbc=pump.jdbcString( "./dump_"+
				ISO8601DateFormat.getDateInstance().format(new Date()));
	}

	protected DataSource getSrcDs() {
		return new  javax.sql.DataSource(){
	
			@Override
			public PrintWriter getLogWriter() throws SQLException {
	
				return null;
			}
	
			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
	
	
			}
	
			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
	
	
			}
	
			@Override
			public int getLoginTimeout() throws SQLException {
	
				return 0;
			}
	
			@Override
			public java.util.logging.Logger getParentLogger()
					throws SQLFeatureNotSupportedException {
	
				return null;
			}
	
			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
	
				return null;
			}
	
			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
	
				return false;
			}
	
			@Override
			public Connection getConnection() throws SQLException {
	
				return  DriverManager.getConnection(srcJdbc,username,pw);
			}
	
			@Override
			public Connection getConnection(String username, String password)
					throws SQLException {
	
				return null;
			}};
	}

	protected DataSource getDestDs() {
		return new  javax.sql.DataSource(){
	
			@Override
			public PrintWriter getLogWriter() throws SQLException {
				return null;
			}
	
			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
	
	
			}
	
			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
	
	
			}
	
			@Override
			public int getLoginTimeout() throws SQLException {
	
				return 0;
			}
	
			@Override
			public java.util.logging.Logger getParentLogger()
					throws SQLFeatureNotSupportedException {
	
				return null;
			}
	
			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
	
				return null;
			}
	
			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
	
				return false;
			}
	
			@Override
			public Connection getConnection() throws SQLException {
	
				return  DriverManager.getConnection(dstJdbc);
			}
	
			@Override
			public Connection getConnection(String username, String password)
					throws SQLException {
	
				return null;
			}};
	}

	public String getDstJdbc() {
		return dstJdbc;
	}

}