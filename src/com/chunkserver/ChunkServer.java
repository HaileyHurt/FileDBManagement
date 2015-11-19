package com.chunkserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;



import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import utilities.Constants;

import com.client.Client;
import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface, Runnable {	
	//Used for the file system
	public static long counter;
	public final static String ConfigFile = "config.txt";
    public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
    public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	
	//Define data for distributed chunkserver
	HashMap<String, RandomAccessFile> fileMap;
	HashMap<String, ChunkInfo> chunkMap;
	Vector<String> chunkHandles;
	Vector<String> ownedLeases;
	String[] filenames;
	HashMap<String, byte[]> appendMap;
	
	String localhostIP;
	String masterIP;
	int masterPort;
	int serverPort;
	
	Socket masterSocket;
	ObjectOutputStream masterWriteStream;
	ObjectInputStream masterReadStream;
	
	ClientListener clientListener;
	ServerSocket clientServerSocket;
	
	private Lock masterWriteLock;
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
//		File dir = new File(filePath);
//		File[] fs = dir.listFiles();
//
//		if(fs.length == 0){
//			counter = 0;
//		}else{
//			long[] cntrs = new long[fs.length];
//			for (int j=0; j < cntrs.length; j++)
//				cntrs[j] = Long.valueOf( fs[j].getName() ); 
//			
//			Arrays.sort(cntrs);
//			counter = cntrs[cntrs.length - 1];
//		}
		
		//initialize data for distributed chunkserver
		try {
			localhostIP = InetAddress.getLocalHost().toString();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		masterWriteLock = new ReentrantLock();
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(Constants.filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(Constants.filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(Constants.filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
//	public static void ReadAndProcessRequests()
//	{
//		ChunkServer cs = new ChunkServer();
//		
//		//Used for communication with the Client via the network
//		int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port 
//		ServerSocket commChanel = null;
//		ObjectOutputStream WriteOutput = null;
//		ObjectInputStream ReadInput = null;
//		
//		try {
//			//Allocate a port and write it to the config file for the Client to consume
//			commChanel = new ServerSocket(ServerPort);
//			ServerPort=commChanel.getLocalPort();
//			PrintWriter outWrite=new PrintWriter(new FileOutputStream(ClientConfigFile));
//			outWrite.println("localhost:"+ServerPort);
//			outWrite.close();
//		} catch (IOException ex) {
//			System.out.println("Error, failed to open a new socket to listen on.");
//			ex.printStackTrace();
//		}
//		
//		boolean done = false;
//		Socket ClientConnection = null;  //A client's connection to the server
//
//		while (!done){
//			try {
//				ClientConnection = commChanel.accept();
//				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
//				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
//				
//				//Use the existing input and output stream as long as the client is connected
//				while (!ClientConnection.isClosed()) {
//					int payloadsize =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
//					if (payloadsize == -1) 
//						break;
//					int CMD = Client.ReadIntFromInputStream("ChunkServer", ReadInput);
//					switch (CMD){
//					case CreateChunkCMD:
//						String chunkhandle = cs.createChunk();
//						byte[] CHinbytes = chunkhandle.getBytes();
//						WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
//						WriteOutput.write(CHinbytes);
//						WriteOutput.flush();
//						break;
//
//					case ReadChunkCMD:
//						int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
//						int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
//						int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
//						if (chunkhandlesize < 0)
//							System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
//						byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
//						String ChunkHandle = (new String(CHinBytes)).toString();
//						
//						byte[] res = cs.readChunk(ChunkHandle, offset, payloadlength);
//						
//						if (res == null)
//							WriteOutput.writeInt(ChunkServer.PayloadSZ);
//						else {
//							WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
//							WriteOutput.write(res);
//						}
//						WriteOutput.flush();
//						break;
//
//					case WriteChunkCMD:
//						offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
//						payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
//						byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
//						chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
//						if (chunkhandlesize < 0)
//							System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
//						CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
//						ChunkHandle = (new String(CHinBytes)).toString();
//
//						//Call the writeChunk command
//						if (cs.writeChunk(ChunkHandle, payload, offset))
//							WriteOutput.writeInt(ChunkServer.TRUE);
//						else WriteOutput.writeInt(ChunkServer.FALSE);
//						
//						WriteOutput.flush();
//						break;
//
//					default:
//						System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
//						break;
//					}
//				}
//			} catch (IOException ex){
//				System.out.println("Client Disconnected");
//			} finally {
//				try {
//					if (ClientConnection != null)
//						ClientConnection.close();
//					if (ReadInput != null)
//						ReadInput.close();
//					if (WriteOutput != null) WriteOutput.close();
//				} catch (IOException fex){
//					System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
//					fex.printStackTrace();
//				}
//			}
//		}
//	}
	
	public void sendHeartBeat(boolean hasLease, String leaseHandle) {
		masterWriteLock.lock();
		try {
			if(hasLease) {
				masterWriteStream.writeInt(Constants.HeartBeatWithLease);
				masterWriteStream.writeLong(Long.parseLong(leaseHandle));
			}
			else masterWriteStream.writeInt(Constants.HeartBeatNoLease);
			
			masterWriteStream.writeInt(chunkHandles.size());
			for(int i = 0; i < chunkHandles.size(); i++) {
				masterWriteStream.writeLong(Long.parseLong(chunkHandles.get(i)));
			}
			
			masterWriteStream.writeInt(serverPort);
			
			masterWriteStream.flush();
		} catch (IOException e1) {
			System.out.println("Error occurred sending heartbeat");
			e1.printStackTrace();
		}
		masterWriteLock.unlock();
	}

	@Override
	public void run() {
		//read in file and chunk meta-data
		fileMap = new HashMap<String, RandomAccessFile>();
		chunkMap = new HashMap<String, ChunkInfo>();
		chunkHandles = new Vector<String>();
		ownedLeases = new Vector<String>();
		appendMap = new HashMap<String, byte[]>();
		
		File dir = new File(Constants.filePath);
		filenames = dir.list();
		for(int i = 0; i < filenames.length; i++) {
			try {
				//create new RAF and add to file map
				RandomAccessFile raf = new RandomAccessFile(Constants.filePath+filenames[i], "rw");
				fileMap.put(filenames[i], raf);
				//add chunks in file to file map
				while(raf.getFilePointer() < raf.length()) {
					long pos = raf.getFilePointer();
					String chunkHandle = Long.toString(raf.readLong());
					chunkHandles.add(chunkHandle);
					if(chunkMap.put(chunkHandle, new ChunkInfo(filenames[i], pos)) != null) {
						//TODO this was a duplicate- look up how to handle duplicates
					}
					//set file pointer to position of next chunk-handle
					raf.seek(pos+ChunkSize);
				}
			} catch (FileNotFoundException e) {
				System.out.println("Error occurred opening chunk file:" + filenames[i]);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Error occurred during I/O of chunk file:" + filenames[i]);
				e.printStackTrace();
			}
		}
		
		//connect to master
		try {
			RandomAccessFile config = new RandomAccessFile(new File("config"), "r");	
			masterIP = config.readLine();
			config.readLine(); //discard line
			masterPort = config.readInt();
		} catch (IOException e1) {
			System.out.println("Error occurred reading config file");
			e1.printStackTrace();
		}
		
		try {
			Socket masterSocket = new Socket(masterIP, masterPort);
			masterWriteStream = new ObjectOutputStream(masterSocket.getOutputStream());
			masterReadStream = new ObjectInputStream(masterSocket.getInputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error occurred connecting to master");
			e.printStackTrace();
		}
		
		serverPort = 0;
		ServerSocket clientServerSocket = null;
		try {
			clientServerSocket = new ServerSocket(serverPort);
		} catch (IOException e1) {
			System.out.println("Error occurred opening clientServerSocket");
			e1.printStackTrace();
		}
		serverPort = clientServerSocket.getLocalPort();
		
		//start client listener to listen for clients
		clientListener = new ClientListener(clientServerSocket);
		clientListener.start();
		
		//send heartbeats
		while(true) {
			sendHeartBeat(false, null);
			//TODO check if leases need renewals and send renewal request
			
			try {
				Thread.sleep(Constants.HeartBeatInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/* Listens for and acts on messages received from the master
	 * 
	 */
	public class MasterListener extends Thread {
		@Override
		public void run() {
			while(true) {
				int CMD = Client.ReadIntFromInputStream("chunkserver:"+localhostIP, masterReadStream);
				switch(CMD) {
				case Constants.SendLease:
					//Master gives ownership of lease to chunkserver
					String leaseHandle = Long.toString(Client.ReadLongFromInputStream("chunkserver:"+localhostIP, masterReadStream));
					long leaseGivenTime = Client.ReadLongFromInputStream("chunkserver:"+localhostIP, masterReadStream);
					ChunkInfo chunk = chunkMap.get(leaseHandle);
					masterWriteLock.lock();
					try {
						if(chunk != null) {
							ownedLeases.add(leaseHandle);
							chunk.setLease(true, leaseGivenTime);
							masterWriteStream.writeInt(Constants.TRUE);
						}
						else {
							masterWriteStream.writeInt(Constants.FALSE);
						}
						masterWriteStream.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}
					masterWriteLock.unlock();
					break;
				case Constants.CreateChunk:
					long newHandle = Client.ReadLongFromInputStream("chunkserver:"+localhostIP, masterReadStream);
					String filename = filenames[0]; //always use first file for now
					RandomAccessFile chunkFile = fileMap.get(filename);
					try {
						long pos = chunkFile.length();
						while(chunkFile.getFilePointer() < chunkFile.length()) {
							//iterate through file looking for reclaimed chunks
							long tempPos = chunkFile.getFilePointer();
							long handle = chunkFile.readLong();
							if(handle == -1) {
								pos = tempPos;
								break;
							}
							chunkFile.seek(chunkFile.getFilePointer()+Constants.ChunkSize);
						}
						
						chunkFile.seek(pos);
						chunkFile.writeLong(newHandle);
						//pad rest of chunk with 0s to indicate empty space
						for(int i = 0; i < Constants.ChunkSize - Long.BYTES; i++) chunkFile.writeInt(0);
						chunkMap.put(Long.toString(newHandle), new ChunkInfo(filename, pos));
					} catch (IOException e) {
						System.out.println("Error occurred while creating chunk");
						e.printStackTrace();
					}
					break;
				case Constants.DeleteChunks:
					int numToDelete = Client.ReadIntFromInputStream("chunkserver:"+localhostIP, masterReadStream);
					for(int i = 0; i < numToDelete; i++) {
						long handle = Client.ReadLongFromInputStream("chunkserver:"+localhostIP, masterReadStream);
						ChunkInfo toDelete = chunkMap.remove(Long.toString(handle));
						RandomAccessFile file = fileMap.get(toDelete.getFilename());
						try {
							file.seek(toDelete.getPosition());
							//set chunkhandle to -1 to indicate this space is reclaimed by FS
							file.writeLong(-1);
						} catch (IOException e) {
							System.out.println("Error occurred while deleting chunk");
							e.printStackTrace();
						}
					}
					break;
				}
			}
		}
	}
	
	public class ClientListener extends Thread {
		private ServerSocket clientServerSocket;
		
		public ClientListener(ServerSocket ss) {
			clientServerSocket = ss;
		}
		
		@Override
		public void run() {
			while(true) {
				//accept client connections and start new client thread
				try {
					Socket newClientSocket = clientServerSocket.accept();
					ObjectOutputStream out = new ObjectOutputStream(newClientSocket.getOutputStream());
					ObjectInputStream in = new ObjectInputStream(newClientSocket.getInputStream());
					int type = Client.ReadIntFromInputStream("ClientListener", in);
					if(type == Constants.IsClient) {
						ClientThread clientThread = new ClientThread(newClientSocket, out, in);
						clientThread.start();
					}
					else if(type == Constants.IsChunkServer) {
						ChunkServerThread csThread = new ChunkServerThread(newClientSocket, out, in);
						csThread.start();
					}
				} catch (IOException e) {
					System.out.println("Error, failed to open client socket to listen on.");
					e.printStackTrace();	
				}
			}
		}		
	}
	
	public class ClientThread extends Thread {
		Socket socket;
		ObjectOutputStream clientWriteStream;
		ObjectInputStream clientReadStream;
		
		Vector<Boolean> secondaryResults = new Vector<Boolean>();
		
		public ClientThread(Socket socket, ObjectOutputStream out, ObjectInputStream in) {
			this.socket = socket;
			clientWriteStream = out;
			clientReadStream = in;
		}
		
		private void readFirstRecord() {
			long handle = Client.ReadLongFromInputStream("client:"+socket.getInetAddress(), clientReadStream);
			ChunkInfo chunk = chunkMap.get(Long.toString(handle));
			try {
				if(chunk == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				
				RandomAccessFile file = fileMap.get(chunk.getFilename());
				if(file == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				
				file.seek(chunk.getPosition() + Long.BYTES); //seek to start of first record
				int recSize = file.readInt();
				if(recSize == 0) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;					
				}
				byte[] data = new byte[recSize];
				file.read(data, 0, recSize);
				clientWriteStream.writeInt(recSize);
				clientWriteStream.write(data);
				clientWriteStream.flush();
			}
			catch (IOException e) {
				e.printStackTrace();
			}	
		}
		
		private void readLastRecord() {
			long handle = Client.ReadLongFromInputStream("client:"+socket.getInetAddress(), clientReadStream);
			ChunkInfo chunk = chunkMap.get(Long.toString(handle));
			try {
				if(chunk == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				
				RandomAccessFile file = fileMap.get(chunk.getFilename());
				if(file == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				
				long endOfChunk = chunk.getPosition() + Constants.ChunkSize;
				file.seek(chunk.getPosition() + Long.BYTES); //seek to start of first record
				//iterate to the last record
				int recSize = file.readInt();
				if(recSize == 0) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;					
				}
				long pos = file.getFilePointer();
				while(true) {					
					file.seek(pos + recSize);
					int nextRecSize = file.readInt();
					if(recSize == 0) break; //last record reached
					else {
						recSize = nextRecSize;
						pos = file.getFilePointer();
						if(pos+recSize >= endOfChunk) break; //end of chunk reached
					}
				}
				file.seek(pos); //seek to position of last record
				byte[] data = new byte[recSize];
				file.read(data, 0, recSize);
				clientWriteStream.writeInt(recSize);
				clientWriteStream.write(data);
				clientWriteStream.flush();
			}
			catch (IOException e) {
				e.printStackTrace();
			}				
		}
		
		private void readNextRecord() {
			long handle = Client.ReadLongFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			int recIndex = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			ChunkInfo chunk = chunkMap.get(Long.toString(handle));
			try {
				if(chunk == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				
				RandomAccessFile file = fileMap.get(chunk.getFilename());
				if(file == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				long endOfChunk = chunk.getPosition() + Constants.ChunkSize;
				file.seek(chunk.getPosition() + Long.BYTES); //seek to start of first record
				//iterate to the requested record index
				for(int i = 0; i <= recIndex; i++) {
					int recSize = file.readInt();
					if(recSize == 0) { //records ended
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;					
					}
					
					file.seek(file.getFilePointer()+recSize);
					if(file.getFilePointer() >= endOfChunk) { //reached end of chunk
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;		
					}
				}
				long pos = file.getFilePointer();
				if(pos >= endOfChunk) {
					clientWriteStream.writeInt(Constants.NOT_IN_CHUNK);
					clientWriteStream.flush();
					return;							
				}
				int recSize = file.readInt();
				if(recSize == 0) { //records ended
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;					
				}
				byte[] data = new byte[recSize];
				file.read(data, 0, recSize);
				clientWriteStream.writeInt(recSize);
				clientWriteStream.write(data);
				clientWriteStream.flush();
			}
			catch (IOException e) {
				e.printStackTrace();
			}							
		}
		
		private void readPrevRecord() {
			long handle = Client.ReadLongFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			int recIndex = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			ChunkInfo chunk = chunkMap.get(Long.toString(handle));
			try {
				if(chunk == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				
				if(recIndex == 0) {
					clientWriteStream.writeInt(Constants.NOT_IN_CHUNK);
					clientWriteStream.flush();
					return;					
				}
				
				RandomAccessFile file = fileMap.get(chunk.getFilename());
				if(file == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				long endOfChunk = chunk.getPosition() + Constants.ChunkSize;
				file.seek(chunk.getPosition() + Long.BYTES); //seek to start of first record
				//iterate to one less than requested record index
				for(int i = 0; i < recIndex-1; i++) {
					int recSize = file.readInt();
					if(recSize == 0) { //records ended
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;					
					}
					
					file.seek(file.getFilePointer()+recSize);
					if(file.getFilePointer() >= endOfChunk) { //reached end of chunk
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;		
					}
				}
				long pos = file.getFilePointer();
				if(pos >= endOfChunk) {
					clientWriteStream.writeInt(Constants.NOT_IN_CHUNK);
					clientWriteStream.flush();
					return;							
				}
				int recSize = file.readInt();
				if(recSize == 0) { //records ended
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;					
				}
				byte[] data = new byte[recSize];
				file.read(data, 0, recSize);
				clientWriteStream.writeInt(recSize);
				clientWriteStream.write(data);
				clientWriteStream.flush();
			}
			catch (IOException e) {
				e.printStackTrace();
			}					
		}
		
		//this method called to store data that client wants to append
		private void addDataToWrite() {
			long handle = Client.ReadLongFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			try {
				if(chunkMap.get(Long.toString(handle)) == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				int recIndex = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
				int recSize = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
				byte[] data = new byte[recSize];
				clientReadStream.read(data, 0, recSize);
				String key = Long.toString(handle) + ":" + Integer.toString(recIndex) + ":" + socket.getInetAddress().toString();
				appendMap.put(key, data);
				clientWriteStream.writeInt(Constants.TRUE);
				clientWriteStream.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		private void appendRecord() {
			long handle = Client.ReadLongFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			int recIndex = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			int numChunkservers = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			//get the data that was received earlier
			String key = Long.toString(handle) + ":" + Integer.toString(recIndex) + ":" + socket.getInetAddress().toString();
			byte[] data = appendMap.get(key);
			ChunkInfo chunk = chunkMap.get(Long.toString(handle));
			try {
				if(chunk == null || data == null || !chunk.hasLease()) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				//if 50 seconds have passed, renew the lease
				if(System.currentTimeMillis() - chunk.getLeaseGivenTime() > 50000) {
					sendHeartBeat(true, Long.toString(handle));
					int response = masterReadStream.readInt();
					if(response != Constants.TRUE) {
						//did not get the lease, return fail
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;						
					}
					chunk.setLease(true, chunk.getLeaseGivenTime()+60000); //extend lease time 60 seconds
				}
				//record IPs and ports of replicas
				String [] IPs = new String[numChunkservers];
				int [] ports = new int[numChunkservers];
				for(int i = 0; i < numChunkservers; i++) {
					int strlen = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
					byte[] buf = new byte[strlen];
					clientReadStream.read(buf, 0, strlen);
					String IP = Arrays.toString(buf);
					int port = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
					IPs[i] = IP;
					ports[i] = port;
				}
				//as the primary, write the data first
				RandomAccessFile file = fileMap.get(chunk.getFilename());
				long endOfChunk = chunk.getPosition() + Constants.ChunkSize;
				file.seek(chunk.getPosition() + Long.BYTES); //seek to start of data
				//iterate to the right index
				for(int i = 0; i < recIndex; i++) {
					int recSize = file.readInt();
					if(recSize == 0) { //records ended
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;					
					}
					
					file.seek(file.getFilePointer()+recSize);
					if(file.getFilePointer() >= endOfChunk) { //reached end of chunk
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;		
					}
				}
				int recSize = file.readInt();
				//check that data size is not bigger than remaining space
				if(recSize != 0 || (data.length > endOfChunk - file.getFilePointer())) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				file.seek(file.getFilePointer() - Integer.BYTES);
				file.writeInt(recSize); //write size of record
				file.write(data, 0, recSize);
				
				for(int i = 0; i < numChunkservers; i++) {
					if(IPs[i] == localhostIP) continue;
					Socket csSocket = new Socket(IPs[i], ports[i]);
					(new Thread() {
						@Override
						public void run() {
							try {
								ObjectOutputStream out = new ObjectOutputStream(csSocket.getOutputStream());
								ObjectInputStream in = new ObjectInputStream(csSocket.getInputStream());
								out.writeInt(Constants.IsChunkServer);
								out.writeInt(Constants.APPEND_RECORD);
								out.writeLong(handle);
								out.writeInt(recIndex);
								out.writeInt(socket.getInetAddress().toString().length());
								out.write(Byte.valueOf(socket.getInetAddress().toString()));
								
								int response = in.readInt();
								if(response == Constants.TRUE) {
									secondaryResults.add(true);
								}
								else {
									secondaryResults.add(false);
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}).start();
				}
				
				int time = 0;
				while(true) { //check for responses, timeout after 10 seconds
					if(secondaryResults.size() == numChunkservers-1) {
						for(int i = 0; i < numChunkservers; i++) {
							if(!secondaryResults.get(i) || time == 10000) {
								//failure
								clientWriteStream.writeInt(Constants.FALSE);
								clientWriteStream.flush();
								return;
								
							}
						}
						//success
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						return;
					}
					try {
						Thread.sleep(100);
						time += 100;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		private void deleteRecord() {
			long handle = Client.ReadLongFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			int recIndex = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), clientReadStream);
			ChunkInfo chunk = chunkMap.get(Long.toString(handle));
			try {
				if(chunk == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				
				RandomAccessFile file = fileMap.get(chunk.getFilename());
				if(file == null) {
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;
				}
				long endOfChunk = chunk.getPosition() + Constants.ChunkSize;
				file.seek(chunk.getPosition() + Long.BYTES);
				//iterate to the right record
				for(int i = 0; i < recIndex; i++) {
					int recSize = file.readInt();
					if(recSize == 0) { //no more records
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;
					}
					
					file.seek(file.getFilePointer() + recSize);
					if(file.getFilePointer() >= endOfChunk) {
						clientWriteStream.writeInt(Constants.FALSE);
						clientWriteStream.flush();
						return;
					}
				}
				long pos = file.getFilePointer(); //position of record size
				int recSize = file.readInt();
				if(recSize == 0) { //records ended
					clientWriteStream.writeInt(Constants.FALSE);
					clientWriteStream.flush();
					return;					
				}
				file.seek(file.getFilePointer() + recSize); //seek past end of record
				int bytesToMove = (int)(endOfChunk - file.getFilePointer());
				byte[] data = new byte[bytesToMove];
				file.read(data, 0, bytesToMove);
				
				file.seek(pos);
				file.write(data, 0, bytesToMove);
				
				int bytesToZeroOut = (int)(endOfChunk - file.getFilePointer());
				for(int i = 0; i < bytesToZeroOut; i++) file.writeInt(0);
				
			} catch(IOException e) {
 				e.printStackTrace();
			}
		}
		
		@Override
		public void run() {
			
			while(true) {
				//process client requests
				int CMD = Client.ReadIntFromInputStream("ChunkServer.ClientThread:"+localhostIP, clientReadStream);
				try {
					switch(CMD) {
					case Constants.PING:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						break;
					case Constants.READ_FIRST_RECORD:
						readFirstRecord();
						break;
					case Constants.READ_LAST_RECORD:
						readLastRecord();
						break;
					case Constants.READ_NEXT_RECORD:
						readNextRecord();
						break;
					case Constants.READ_PREV_RECORD:
						readPrevRecord();
						break;					
					case Constants.DATA_TO_WRITE:
						addDataToWrite();
						break;
					case Constants.APPEND_RECORD:
						appendRecord();
						break;
					case Constants.DELETE_RECORD:
						deleteRecord();
						break;
					default:
						break;
					}
				} catch(IOException e) {
					System.out.println("Error, client connection failed");
					e.printStackTrace();					
				} finally {
					
				}
			}
		}
	}
	
	/* for receiving messages from another chunkserver */
	public class ChunkServerThread extends Thread {
		private Socket socket;
		private ObjectOutputStream csWriteStream;
		private ObjectInputStream csReadStream;
		
		public ChunkServerThread(Socket socket, ObjectOutputStream out, ObjectInputStream in) {
			this.socket = socket;
			csWriteStream = out;
			csReadStream = in;
		}
		
		private void secondaryAppend() {
			long handle = Client.ReadLongFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), csReadStream);
			int recIndex = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), csReadStream);
			int strlen = Client.ReadIntFromInputStream("chunkserver ClientThread:"+socket.getInetAddress(), csReadStream);
			byte [] buf = new byte[strlen];
			String IP = Arrays.toString(buf);
			String key = Long.toString(handle) + ":" + Integer.toString(recIndex) + ":" + IP;
			byte[] data = appendMap.get(key);
			ChunkInfo chunk = chunkMap.get(Long.toString(handle));
			try {
				if(chunk == null || data == null) {
					csWriteStream.writeInt(Constants.FALSE);
					csWriteStream.flush();
					return;
				}
				//write the data
				RandomAccessFile file = fileMap.get(chunk.getFilename());
				long endOfChunk = chunk.getPosition() + Constants.ChunkSize;
				file.seek(chunk.getPosition() + Long.BYTES); //seek to start of data
				//iterate to the right index
				for(int i = 0; i < recIndex; i++) {
					int recSize = file.readInt();
					if(recSize == 0) { //records ended
						csWriteStream.writeInt(Constants.FALSE);
						csWriteStream.flush();
						return;					
					}
					
					file.seek(file.getFilePointer()+recSize);
					if(file.getFilePointer() >= endOfChunk) { //reached end of chunk
						csWriteStream.writeInt(Constants.FALSE);
						csWriteStream.flush();
						return;		
					}
				}
				int recSize = file.readInt();
				//check that data size is not bigger than remaining space
				if(recSize != 0 || (data.length > endOfChunk - file.getFilePointer())) {
					csWriteStream.writeInt(Constants.FALSE);
					csWriteStream.flush();
					return;
				}
				file.seek(file.getFilePointer() - Integer.BYTES);
				file.writeInt(recSize); //write size of record
				file.write(data, 0, recSize);
				
				csWriteStream.writeInt(Constants.TRUE);
				csWriteStream.flush();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void run() {
			try {
				int CMD = csReadStream.readInt();
				switch(CMD) {
				case Constants.APPEND_RECORD:
					secondaryAppend();
					break;
				default:
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/* Stores metadata about a chunk, namely the file that
	 * it is stored in, its position, in bytes, in that file,
	 * and whether it currently owns the lease.
	 */
	public class ChunkInfo {
		//chunk: first 8 bytes are the chunkhandle, rest is data
		
		private String filename;
		private long position; //start of chunk
		private boolean lease;
		private long leaseGivenTime;
		private Lock leaseLock;
		public ChunkInfo(String filename, long position) {
			this.filename = filename;
			this.position = position;
			lease = false;
			leaseLock = new ReentrantLock();
		}
		//TODO maybe needs setters & synchronization (like, if you change the file/position the chunk is in)
		public String getFilename() { return filename; }
		public long getPosition() { return position; }
		
		public boolean hasLease() {
			boolean val = false;
			leaseLock.lock();
			val = lease;
			leaseLock.unlock();
			return val;
		}
		
		public long getLeaseGivenTime() {
			long time = 0;
			leaseLock.lock();
			time = leaseGivenTime;
			leaseLock.unlock();
			return time; 
		}
		
		public void setLease(boolean val, long time) {
			leaseLock.lock();
			lease = val;
			leaseGivenTime = time;
			leaseLock.unlock();
		}
	}
	
	public static void main(String args[])
	{
//		ReadAndProcessRequests();
		(new Thread(new ChunkServer())).start();
	}

}
