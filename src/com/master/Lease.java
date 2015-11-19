package com.master;

public class Lease {
	
	private String server;
	private long time;
	
	public Lease (String server){
		this.server = server;
		time = System.currentTimeMillis();
	}
	
	public long getTime(){
		return time;
	}
	
	public String getServer(){
		return server;
	}
	
	public void updateTime(){
		time = System.currentTimeMillis();
	}

}
