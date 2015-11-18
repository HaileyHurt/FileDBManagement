package com.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import com.client.Client;

public class ServerThreadCom implements Runnable {
	private Socket ServerConnection;
	private DirectoryManager dirManager;
	
	public ServerThreadCom (Socket ServerConnection, DirectoryManager dirManager){
		this.ServerConnection = ServerConnection;
		this.dirManager = dirManager;
	}
	
	public void run (){
		try {
			ObjectOutputStream WriteOutput = new ObjectOutputStream(ServerConnection.getOutputStream());
			ObjectInputStream ReadInput = new ObjectInputStream(ServerConnection.getInputStream());
			
			while (!ServerConnection.isClosed()){
				int operation = Client.ReadIntFromInputStream("ServerThreadCom", ReadInput);
				System.out.println ("Operation: " + operation);
				
				switch (operation){
					case Master.HEART_BEAT_MESSAGE:
					
					case -1:
					default:
						System.out.println("Error.. (ServerThreadCom): Invalid operation!");
				}
			}

			ReadInput.close();
			WriteOutput.close();
			ServerConnection.close();
		}
		catch (Exception e){
			System.out.println ("Error at Master:ServerThreadCom");
			e.printStackTrace();
		}
	}
	
}
