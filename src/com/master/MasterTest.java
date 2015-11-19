package com.master;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;

import com.client.Client;

import utilities.Constants;

public class MasterTest {
	private static int port;
	
	public static void main(String[] args) {
		try {			
			BufferedReader bufferedReader = new BufferedReader(new FileReader(Constants.ConfigFile));
			String line;
	        line = bufferedReader.readLine();
	        line = bufferedReader.readLine();
	        port = Integer.parseInt(line);			
			
			Scanner in = new Scanner(System.in);
			System.out.println ("1 -> Create Dir");
			System.out.println ("2 -> Delete Dir");
			System.out.println ("3 -> Rename Dir");
			System.out.println ("4 -> Create File");
			//System.out.println ("5 -> Delete File");
			//System.out.println ("6 -> Create Chunk");
			
			System.out.println ("Port: " + port);
			int operation = Integer.parseInt(in.nextLine());
			while (true){
				switch (operation){
					case 0:
						break;
					case 1:
						System.out.println("Calling the createdir function");
						String creSrc = in.nextLine();
						String creDir = in.nextLine();
						createDirTest(creSrc, creDir);
						break;
					case 2:
						System.out.println("Calling the deletedir function");
						String delSrc = in.nextLine();
						String dirDir = in.nextLine();
						deleteDirTest(delSrc, dirDir);
						break;
					case 3:
						System.out.println("Calling the renamedir function");
						String renSrc = in.nextLine();
						String renDir = in.nextLine();
						renameDirTest(renSrc, renDir);
						break;
					case 4:
						System.out.println("Calling the createfile function");
						String crefSrc = in.nextLine();
						String crefDir = in.nextLine();
						createFileTest(crefSrc, crefDir);
						break;
					default:
						break;
				}
				operation = Integer.parseInt(in.nextLine());
			}
		}
		catch (Exception e){
			System.out.println ("Error: (Master) main class.");			
			e.printStackTrace();
		}
	}
	
	public static void createDirTest (String src, String dir) throws Exception {
		Socket s1 = new Socket ("localhost", port);
		
		ObjectOutputStream WriteOutput = new ObjectOutputStream(s1.getOutputStream());
		ObjectInputStream ReadInput = new ObjectInputStream(s1.getInputStream());
		
		WriteOutput.writeInt(Constants.CREATE_DIR);	

		byte[] payload1 = (src.getBytes());
		
		WriteOutput.writeInt(payload1.length);
		WriteOutput.write(payload1);
		
		
		String dirName = dir;
		byte[] payload2 = dirName.getBytes();
		
		WriteOutput.writeInt(payload2.length);
		WriteOutput.write(payload2);
		WriteOutput.flush();
		
		int resultOfOperation = Client.ReadIntFromInputStream("MasterTest", ReadInput);
		// System.out.println ("Res: " + resultOfOperation);
		
		ReadInput.close();
		WriteOutput.close();
		s1.close();
	}
	
	public static void deleteDirTest (String src, String dir) throws Exception {
		Socket s1 = new Socket ("localhost", port);
		
		ObjectOutputStream WriteOutput = new ObjectOutputStream(s1.getOutputStream());
		ObjectInputStream ReadInput = new ObjectInputStream(s1.getInputStream());
		
		WriteOutput.writeInt(Constants.DELETE_DIR);	

		byte[] payload1 = (src.getBytes());
		
		WriteOutput.writeInt(payload1.length);
		WriteOutput.write(payload1);
		
		String dirName = dir;
		byte[] payload2 = dirName.getBytes();
		
		WriteOutput.writeInt(payload2.length);
		WriteOutput.write(payload2);
		WriteOutput.flush();
		
		int resultOfOperation = Client.ReadIntFromInputStream("MasterTest", ReadInput);
		// System.out.println ("Res: " + resultOfOperation);
		
		ReadInput.close();
		WriteOutput.close();
		s1.close();
	}
	
	public static void renameDirTest (String src, String dir) throws Exception {
		Socket s1 = new Socket ("localhost", port);
		
		ObjectOutputStream WriteOutput = new ObjectOutputStream(s1.getOutputStream());
		ObjectInputStream ReadInput = new ObjectInputStream(s1.getInputStream());
		
		WriteOutput.writeInt(Constants.RENAME_DIR);	

		byte[] payload1 = (src.getBytes());
		
		WriteOutput.writeInt(payload1.length);
		WriteOutput.write(payload1);
		
		String dirName = dir;
		byte[] payload2 = dirName.getBytes();
		
		WriteOutput.writeInt(payload2.length);
		WriteOutput.write(payload2);
		WriteOutput.flush();
		
		int resultOfOperation = Client.ReadIntFromInputStream("MasterTest", ReadInput);
		// System.out.println ("Res: " + resultOfOperation);
		
		ReadInput.close();
		WriteOutput.close();
		s1.close();
	}
	
	public static void createFileTest (String src, String filename) throws Exception {
		Socket s1 = new Socket ("localhost", port);
		
		ObjectOutputStream WriteOutput = new ObjectOutputStream(s1.getOutputStream());
		ObjectInputStream ReadInput = new ObjectInputStream(s1.getInputStream());
		
		WriteOutput.writeInt(Constants.CREATE_FILE);	

		byte[] payload1 = (src.getBytes());
		
		WriteOutput.writeInt(payload1.length);
		WriteOutput.write(payload1);
		
		String dirName = filename;
		byte[] payload2 = dirName.getBytes();
		
		WriteOutput.writeInt(payload2.length);
		WriteOutput.write(payload2);
		WriteOutput.flush();
		
		int resultOfOperation = Client.ReadIntFromInputStream("MasterTest", ReadInput);
		// System.out.println ("Res: " + resultOfOperation);
		
		ReadInput.close();
		WriteOutput.close();
		s1.close();
	}

}