package com.master;

public class Master {

	public final static String configFile = "config.txt";
	public static final int CREATE_DIR = 1;
	public static final int DELETE_DIR = 2;
	public static final int RENAME_DIR = 3;
	public static final int LIST_DIR = 4;
	public static final int CREATE_FILE = 5;
	public static final int DELETE_FILE = 6;
	public static final int OPEN_FILE = 7;
	public static final int CLOSE_FILE = 8;
	public static final int REGISTRATION_MESSAGE = 9;
	public static final int HEART_BEAT_MESSAGE = 10;
	
	public static void main (String args[]){
		try {
		DirectoryManager dirManager = new DirectoryManager();
		
		Thread clientThread = new Thread(new ClientThread(dirManager));
		clientThread.start();
		
		Thread.sleep(500);
		
		Thread serverThread = new Thread(new ServerThread(dirManager));
		serverThread.start();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}	
}