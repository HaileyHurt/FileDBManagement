package com.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import com.client.Client;

public class ServerThreadCom implements Runnable {
	
	private ChunkToServers chunksmap;
	private ArrayList <String> servers;
	
	private Socket ServerConnection;
	private ObjectInputStream ReadInput;
	private ObjectOutputStream WriteOutput; // for the communication with the server
		
	public ServerThreadCom (Socket ServerConnection, ChunkToServers chunksmap, ArrayList <String> servers){
		this.ServerConnection = ServerConnection;
		this.chunksmap = chunksmap;
		this.servers = servers;
	}
	
	public void run (){
		try {
			ReadInput = new ObjectInputStream(ServerConnection.getInputStream());
			WriteOutput = new ObjectOutputStream(ServerConnection.getOutputStream());
			
			while (!ServerConnection.isClosed()){
				int operation =  Client.ReadIntFromInputStream("ServerThreadCom", ReadInput);
				System.out.println ("Operation: " + operation);
				
				switch (operation){
					case Master.REGISTRATION_MESSAGE:
						// read the number of chunks
						// read the chunkname size
						// read the chunkname... and so on
					default:
						System.out.println("Error.. (ServerThreadCom): Invalid operation!");
				}
			}
			
		}
		catch (Exception e){
			System.out.println ("Error at Master:ServerThreadCom");
			e.printStackTrace();
		}
	}
	
}
