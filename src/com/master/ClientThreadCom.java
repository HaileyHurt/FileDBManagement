package com.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.client.Client;
import com.client.ClientFS.FSReturnVals;

import utilities.Constants;

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
					case Constants.CREATE_DIR:
						createDirProcedure(WriteOutput, ReadInput);
						ClientConnection.close();
						break;
					case Constants.DELETE_DIR:
						deleteDirProcedure(WriteOutput, ReadInput);
						ClientConnection.close();
						break;
					case Constants.RENAME_DIR:
						renameDirProcedure(WriteOutput, ReadInput);
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
				
				dirManager.print();
				System.out.println("\n\n");
			}
			
			ReadInput.close();
			WriteOutput.close();
		}
		catch (Exception e){
			System.out.println ("Error at Master:ClientThreadCom");
			e.printStackTrace();
		}
	 }
	 
	 public void createDirProcedure(ObjectOutputStream WriteOutput, ObjectInputStream ReadInput) throws IOException{
		int srcSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
		byte[] srcPath = Client.RecvPayload("ClientThreadCom", ReadInput, srcSize);
		
		String strFilePath = new String (srcPath);
		int dirSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
		byte[] dirName = Client.RecvPayload("ClientThreadCom", ReadInput, dirSize);
		String strDirName = new String (dirName);
		
		boolean result = dirManager.createDir(strFilePath, strDirName);
		
		if (result == true){
			WriteOutput.writeInt(1);
		}
		else {
			WriteOutput.writeInt(0);
		}
		
		WriteOutput.flush();
	 }

	 public void deleteDirProcedure(ObjectOutputStream WriteOutput, ObjectInputStream ReadInput) throws IOException{
		int srcSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
		byte[] srcPath = Client.RecvPayload("ClientThreadCom", ReadInput, srcSize);
		
		String strPath = new String (srcPath);
		
		int dirSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
		byte[] dirName = Client.RecvPayload("ClientThreadCom", ReadInput, dirSize);
		String strDirName = new String (dirName);
		
		boolean result = dirManager.deleteDir(strPath, strDirName);
		
		if (result == true){
			WriteOutput.writeInt(1);
		}
		else {
			WriteOutput.writeInt(0);
		}
		
		WriteOutput.flush();
	 }
	 
	 public void renameDirProcedure(ObjectOutputStream WriteOutput, ObjectInputStream ReadInput) throws IOException{
			int srcSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
			byte[] srcPath = Client.RecvPayload("ClientThreadCom", ReadInput, srcSize);
			
			String strPath = new String (srcPath);
			
			int dirSize = Client.ReadIntFromInputStream("ClientThreadCom", ReadInput);
			byte[] dirName = Client.RecvPayload("ClientThreadCom", ReadInput, dirSize);
			String strDirName = new String (dirName);
			
			boolean result = dirManager.renameDir(strPath, strDirName);
			
			if (result == true){
				WriteOutput.writeInt(1);
			}
			else {
				WriteOutput.writeInt(0);
			}
			
			WriteOutput.flush();
		 }
}
