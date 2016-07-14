package com.gioorgi.smartsync.rest;

import java.io.Serializable;

public class SyncReport implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	float version;

	public float getVersion() {
		return version;
	}

	public void setVersion(float version) {
		this.version = version;
	}
	
	
}
