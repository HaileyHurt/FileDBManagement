package com.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.client.ClientFS.FSReturnVals;

import utilities.Constants;

public class DirectoryManager {
	private static long chunkHandle;
	private Directory root;
	private Map<String, ArrayList<String>> chunks; // maping chunks to servers
	private Map<String, Lease> leases; // maping chunks to Lease
	private ArrayList<String> servers;
	private ArrayList<String> operations;
	
	public DirectoryManager (){
		chunkHandle = 1;
		root = new Directory();
		operations = new ArrayList<String>();
		leases = new HashMap<String, Lease>();
		servers = new ArrayList<String>();
	}
	
	public DirectoryManager (String logFile){
		chunkHandle = 1;
		root = new Directory();
		operations = new ArrayList<String>();
		leases = new HashMap<String, Lease>();
		servers = new ArrayList<String>();
		
		recoverFromLog(logFile);
		
	}
	
	public void recoverFromLog(String logFile){
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile));
			
			String line;
			while((line = bufferedReader.readLine()) != null) {
	            String splittedLine[] = line.split(";");
	            if (splittedLine[0].equals("createDir")){
	            	createDir(splittedLine[1], splittedLine[2]);
	            }	            
	            if (splittedLine[0].equals("deleteDir")){
	            	deleteDir(splittedLine[1], splittedLine[2]);
	            }	            
	            if (splittedLine[0].equals("renameDir")){
	            	renameDir(splittedLine[1], splittedLine[2]);
	            }	            
	            if (splittedLine[0].equals("createFile")){
	            	createFile(splittedLine[1], splittedLine[2]);
	            }	            
	            if (splittedLine[0].equals("deleteFile")){
	            	deleteFile(splittedLine[1], splittedLine[2]);
	            }	            
	            if (splittedLine[0].equals("createChunk")){
	            	deleteFile(splittedLine[1], splittedLine[2]);
	            }	            
	        }
			
			
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public synchronized void storeInLog(){
		try {
			Random rand = new Random();
			int randomNumber = rand.nextInt(100) + 1;
			if (randomNumber <= Constants.MASTER_PROBABILITY){
				PrintWriter outWrite = new PrintWriter(new BufferedWriter(new FileWriter(Constants.masterLogFile, true)));
				
				for (int i = 0; i < operations.size(); i++){
					outWrite.println(operations.get(i));
				}
				
				operations = new ArrayList<String>();
				outWrite.close();
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public boolean createDir(String src, String dirname) {		
		Directory target = getDirectory(src);
		if (target != null){
			boolean result = target.createDir(dirname);
			
			if (result){
				operations.add ("createDir;" + src + ";" + dirname);
				storeInLog();
			}
			
			return result;
		}
		
		return false; // FSReturnVals.SrcDirNotExistent;
	}
	
	public boolean deleteDir(String src, String dirname) {
		Directory target = getDirectory(src);
		if (target != null){
			boolean result = target.deleteDir(dirname);
			if (result){
				operations.add ("deleteDir;" + src + ";" + dirname);
				storeInLog();
			}
			
			return result;
		}
		
		return false; // FSReturnVals.SrcDirNotExistent;
	}
	
	public boolean renameDir(String src, String dirname) {
		Directory target = getDirectory(src);
		
		if (target != null){
			target.setDirectoryName(dirname);
			
			operations.add ("renameDir;" + src + ";" + dirname);
			storeInLog();
			
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
			boolean result = target.createFile(filename);
			if (result){
				operations.add ("createFile;" + src + ";" + filename);
				storeInLog();
			}
			
			return result;
		}
		
		return false;
	}
	
	public boolean deleteFile(String src, String filename) {
		Directory target = getDirectory(src);
		
		if (target != null){
			boolean result = target.deleteFile(filename);
			
			if (result){
				operations.add ("deleteFile;" + src + ";" + filename);
				storeInLog();
			}
			
			return result;
			
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
	
	public boolean createChunk (String src, String filename){
		Directory target = getDirectory(src);
		
		if (target != null){
			TinyFSFile tfs = target.getFile(filename);
			tfs.addChunk("" + chunkHandle);
			chunkHandle++;
			
			operations.add("createChunk;" + src + ";" + filename);
			storeInLog();
			
			return true;
		}
		
		return false;
	}
	
}
