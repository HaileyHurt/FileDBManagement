package com.master;

import java.util.ArrayList;
import java.util.Map;

public class DirectoryManager {
	private static long chunkHandle;
	private Directory root;
	private Map<String, ArrayList<String>> chunks; // maping chunks to servers
	private Map<String, Lease> leases; // maping chunks to Lease
	private ArrayList<String> servers;
	
	public DirectoryManager (){
		chunkHandle = 1;
		root = new Directory();
	}
	
	public boolean createDir(String src, String dirname) {
		Directory target = getDirectory(src);
		if (target != null){
			target.createDir(dirname);
			return true;
		}
		
		return false;
	}
	
	public boolean deleteDir(String src, String dirname) {
		Directory target = getDirectory(src);
		
		if (target != null){
			target.deleteDir(dirname);
			return true;
		}
		
		return false;
	}
	
	public boolean renameDir(String src, String dirname) {
		Directory target = getDirectory(src);
		
		if (target != null){
			target.setDirectoryName(dirname);
			return true;
		}
		
		return false;
	}
	
	public String[] listDir(String src) {
		Directory target = getDirectory(src);
		
		if (target != null){
			return target.listDir();
		}
		
		return null;
	}
	
	public boolean createFile(String src, String filename) {
		Directory target = getDirectory(src);
		
		if (target != null){
			target.createFile(filename);
			return true;
		}
		
		return false;
	}
	
	public boolean deleteFile(String src, String filename) {
		Directory target = getDirectory(src);
		
		if (target != null){
			target.deleteFile(filename);
			return true;
		}
		
		return false;
	}
	
	// getChunks("/CSCI485/test", "file1.txt"); gonna return all chunks in that file
	public String[] getChunks(String src, String filename) {
		Directory target = getDirectory(src);
		
		if (target != null){
			return target.openFile(filename);
		}
		
		return null;
	}
	
	public ArrayList<String> getServers (String chunkname){
		return chunks.get(chunkname);
	}
	
	public boolean removeServer (String chunkname, String serverIpAddress){
		ArrayList<String> list = chunks.get(chunkname);
		
		if (list == null){
			return false;
		}
		
		for (int i = 0; i < list.size(); i++){
			if (list.get(i).equals(serverIpAddress)){
				list.remove(i);
			}
		}
		
		chunks.put(chunkname, list);
		return true;
	}
	
	public void removeChunks(String[] chunknames){
		for (int i = 0; i < chunknames.length; i++){
			chunks.remove(chunknames[i]);
		}
	}
	
	
	public void removeChunk(String chunkname){
		chunks.remove(chunkname);
	}
	
	public Directory getDirectory(String src){
		String splitSRC[] = src.split("/");
		
		Directory target = root;
		for (int i = 1; i < splitSRC.length && target != null; i++){
			target = target.returnDirectory(splitSRC[i]);
		}
		
		return target;
	}
	
	public void print (){
		root.print();
	}
	
}
