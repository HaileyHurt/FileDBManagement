package com.master;

public class Master {

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