package com.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DirectoryTree {

	private Map<String, ArrayList<String>> directory;
	
	public DirectoryTree(){
		this.directory = new HashMap <String, ArrayList<String>>();
	}
	
	public synchronized boolean createDir (String path, String dirName){
		ArrayList <String> tempList = directory.get(path);
		
		if (tempList != null){
			for (int i = 0; i < tempList.size(); i++){
				if (tempList.get(i).equals(dirName)){
					return false; // directory already exists
				}
			}
		}
		else {
			return false;
		}
		
		tempList.add(dirName);
		ArrayList <String> emptyList = new ArrayList<String>();
		
		directory.put (path, tempList);
		directory.put (dirName, emptyList);
		
		return true;
	}
	
	public synchronized boolean deleteDir (String path, String dirName){
		ArrayList <String> tempDirNameList = directory.get(dirName);
		if (tempDirNameList == null){
			return true;
		}
		else {
			directory.remove(dirName);
			
			ArrayList <String> pathList = directory.get(path);
			ArrayList <String> newPathList = new ArrayList<String>();
			for (int i = 0; i < pathList.size(); i++){
				if (!pathList.get(i).equals(dirName)){
					newPathList.add(pathList.get(i));
				}
			}
			
			directory.put(path, newPathList);
			return true;
		}
	}
	
	public synchronized ArrayList<String> listDir (String path){
		ArrayList <String> tempList = directory.get(path);
		if (tempList == null){
			// throw exception?
		}
		
		return tempList;
	}
}
