package com.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.client.ClientFS;

public class Master {

	public static final int CREATE_DIR = 1;
	public static final int DELETE_DIR = 2;
	public static final int RENAME_DIR = 3;
	public static final int LIST_DIR = 4;
	public static final int CREATE_FILE = 5;
	public static final int DELETE_FILE = 6;
	public static final int OPEN_FILE = 7;
	public static final int CLOSE_FILE = 8;
	public static final int REGISTRATION_MESSAGE = 9;
	
	public static void main (String args[]){
		Map<String, ArrayList<String>> directory = new HashMap <String, ArrayList<String>>();
		ArrayList<String> test = new ArrayList<String>();
		
		directory.put("test1", test);
		directory.get("test1").add("haa");
		System.out.println(directory.get("test1").size());
		System.out.println(directory.get("test1").size()); // just a few tests to see how map works
		
		ArrayList<String> test2 = directory.get("test1");
		test2.add("asdasd");
		
		directory.put("test1", test2);
		System.out.println(directory.get("test1").size());
	}
	
}