package com.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.client.Client;

public class MasterTest {

	public static void main(String[] args) {
		try {
			
			DirectoryManager dm = new DirectoryManager();
			boolean a = dm.createDir("/", "jdbratti");
			boolean b = dm.createDir("/jdbratti", "jdbc");
			boolean asd = dm.createDir("/jdbratti", "jdbc2");
			boolean asdd = dm.createDir("/jdbratti", "jdbc3");
			boolean c = dm.renameDir("/jdbratti", "juvenal");
			
			
			String s[] = dm.listDir("/juvenal");
			for (int i = 0; i < s.length; i++){
				System.out.println("Dir " + (i + 1) + ": " + s[i]);
			}
			
			//boolean c = dm.createDir("/", "jdbc");
			
			
			/*
			Socket s1 = new Socket ("localhost", 50000);
			ObjectOutputStream WriteOutput = new ObjectOutputStream(s1.getOutputStream());
			ObjectInputStream ReadInput = new ObjectInputStream(s1.getInputStream());
			
			WriteOutput.writeInt(Master.CREATE_DIR);	

			byte[] payload1 = ("/".getBytes());
			
			WriteOutput.writeInt(payload1.length);
			WriteOutput.write(payload1);
			
			
			String dirName = "dnine";
			byte[] payload2 = dirName.getBytes();
			
			WriteOutput.writeInt(payload2.length);
			WriteOutput.write(payload2);
			WriteOutput.flush();
			
			int resultOfOperation = Client.ReadIntFromInputStream("MasterTest", ReadInput);
			System.out.println ("Res: " + resultOfOperation);
			
			ReadInput.close();
			WriteOutput.close();
			s1.close();
			*/
		}
		catch (Exception e){
			System.out.println ("Error: (Master) main class.");			
			e.printStackTrace();
		}
	}
}