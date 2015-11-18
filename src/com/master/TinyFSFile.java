package com.master;

import java.util.ArrayList;

public class TinyFSFile {
	
	private String filename;
	private ArrayList<String> chunks;
	
	public TinyFSFile(String []chunkList){
		chunks = new ArrayList <String>();
		for (int i = 0; i < chunkList.length; i++){
			chunks.add(chunkList[i]);
		}
	}
	
	public TinyFSFile(){
		chunks = new ArrayList <String>();
	}
	
	public synchronized String getFilename(){
		return filename;
	}
	
	public synchronized void setFilename(String filename){
		this.filename = filename;
	}
	
	public synchronized ArrayList<String> getChunks(){
		return chunks;
	}
	
	public synchronized String[] getChunksStr(){
		String list[] = new String[chunks.size()];
		for (int i = 0; i < chunks.size(); i++){
			list[i] = chunks.get(i);
		}
		
		return list;
	}
	
	public synchronized void deleteChunk(String chunkName){
		for (int i = 0; i < chunks.size(); i++){
			if (chunks.get(i).equals(chunkName)){
				chunks.remove(i);
			}
		}
	}
	
	public synchronized void addChunk(String chunkName){
		chunks.add(chunkName);
	}
	
}
