package com.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.client.Client;

public class ClientThreadCom implements Runnable {
	private DirectoryManager dirManager;
	private Socket ClientConnection;
		
	public ClientThreadCom (Socket ClientConnection, DirectoryManager directory){
		this.ClientConnection = ClientConnection;
		this.dirManager = directory;	
	}
	
	 public void run (){
		try {			
			ObjectOutputStream WriteOutput = new ObjectOutputStream(this.ClientConnection.getOutputStream());
			ObjectInputStream ReadInput = new ObjectInputStream(this.ClientConnection.getInputStream());
			
			while (!ClientConnection.isClosed()){
				int operation = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
				switch (operation){
					case Master.CREATE_DIR:
						int srcSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
						byte[] filePath = Client.RecvPayload("ClientThreadCom", ReadInput, srcSize);
						String strFilePath = new String (filePath);
						int dirSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
						byte[] dirName = Client.RecvPayload("ClientThreadCom", ReadInput, dirSize);
						String strDirName = new String (dirName);
						
						boolean result = dirManager.createDir(strFilePath, strDirName);
						int resultInt = 0;
						if (result){
							resultInt = 1;
						}
						
						WriteOutput.writeInt(resultInt);
						WriteOutput.flush();
						ClientConnection.close();
						break;
					case -1:
						ClientConnection.close();
						break;
					default:
						System.out.println("Error.. (ClientThreadCom): Invalid operation!");
						ClientConnection.close();						
						break;
				}
			}
			
			ReadInput.close();
			WriteOutput.close();
		}
		catch (Exception e){
			System.out.println ("Error at Master:ClientThreadCom");
			e.printStackTrace();
		}
	 }

}
