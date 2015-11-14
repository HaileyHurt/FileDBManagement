package com.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ChunkToServers {

	private Map<String, ArrayList<String>> chunksmap;
	
	public ChunkToServers(){
		this.chunksmap = new HashMap <String, ArrayList<String>>();
	}
	
	public synchronized ArrayList<String> getServers (String chunk){
		return chunksmap.get(chunk);
	}
	
	public synchronized void addServer (String chunk, String server){
		ArrayList<String> chunkservers = chunksmap.get(chunk);
		if (chunkservers == null){
			ArrayList<String> newchunkservers = new ArrayList<String>();
			newchunkservers.add(server);
			chunksmap.put(chunk, newchunkservers);
		}
		else {
			chunkservers.add(server);
			chunksmap.put(chunk, chunkservers);
		}
	}
	
}
