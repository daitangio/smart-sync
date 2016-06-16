package com.nttdata.gundam;

import org.restlet.Server;
import org.restlet.data.*;
import org.restlet.resource.*;

public class SmartSyncDashboard extends ServerResource {  

	   public static void main(String[] args) throws Exception {  
	      // Create the HTTP server and listen on port 8182  
	      new Server(Protocol.HTTP, 7070, SmartSyncDashboard.class).start();  
	   }

	   @Get  
	   public String toString() {  
	      return "hello, world";  
	   }

	}  