package com.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.client.Client;

public class ClientThreadCom implements Runnable {
	private DirectoryTree directory;
	private Socket ClientConnection;
	private ObjectInputStream ReadInput;
	private ObjectOutputStream WriteOutput; // for the communication with the client
		
	public ClientThreadCom (Socket ClientConnection, DirectoryTree directory){
		this.ClientConnection = ClientConnection;
		this.directory = directory;
	}
	
	 public void run (){
		try {
			ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
			WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
			
			while (!ClientConnection.isClosed()){
				int operation =  Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
				System.out.println ("Operation: " + operation);
				
				switch (operation){
					case Master.CREATE_DIR:
						int srcSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
						byte[] filePath = Client.RecvPayload("ClientThreadCom", ReadInput, srcSize);
						String strFilePath = new String (filePath);
						
						int dirSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
						byte[] dirName = Client.RecvPayload("ClientThreadCom", ReadInput, dirSize);
						String strDirName = new String (dirName);
						
						directory.createDir(strFilePath, strDirName);
						
					default:
						System.out.println("Error.. (ClientThreadCom): Invalid operation!");
				}
			}
			
		}
		catch (Exception e){
			System.out.println ("Error at Master:ClientThreadCom");
			e.printStackTrace();
		}
	 }

}
