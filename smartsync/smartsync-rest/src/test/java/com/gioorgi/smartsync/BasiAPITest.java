package com.gioorgi.smartsync;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

import com.gioorgi.smartsync.rest.SyncReport;

public class BasiAPITest {

	RestTemplate rt= new RestTemplate();
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	@Test
	public void testConnect() {
		SyncReport sr=rt.getForObject("http://localhost:4000/smartsync-rest/rpc/sys/status", 
				SyncReport.class);
		assertEquals(1.0f,sr.getVersion(),0);
		System.out.println(""+sr);
	}

}
