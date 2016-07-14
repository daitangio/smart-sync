package com.gioorgi.smartsync.rest;

import java.io.Serializable;
import java.util.*;

import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.nttdata.gundam.DBCopy2SQLite;

import org.springframework.web.bind.annotation.*;

@Lazy(false)
@RestController
@RequestMapping("/sys")
public class SmartSync {

	
	@RequestMapping(value = "/copy2sqlite", method= RequestMethod.GET, 
			produces = "application/json")	
	public SyncReport copy2Sqlite(String jdbcSrc, String username, String password, List<String> tableList)
	{
		SyncReport r= new SyncReport();		
		Class.forName("org.sqlite.JDBC");
		String extractQuery;
		(new DBCopy2SQLite(jdbcSrc, username,password,tableList)).copy();
	}
	
	@RequestMapping(value = "/status", method= RequestMethod.GET, 
						produces = "application/json")	
	public SyncReport status(){
		SyncReport r= new SyncReport();
		r.setVersion(1.0f);
		return r;		
	}
}
