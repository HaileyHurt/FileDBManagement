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
//import java.util.Arrays;



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
	
	
	//Define data for distributed chunkserver
	HashMap<String, RandomAccessFile> fileMap;
	HashMap<String, ChunkInfo> chunkMap;
	Vector<String> chunkHandles;
	Vector<String> ownedLeases;
	
	String localhostIP;
	String masterIP;
	int masterPort;
	
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
			masterWriteStream.flush();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		masterWriteLock.unlock();
	}
	
	public void sendLease(long chunkhandle) {
		//asdf
		
	}

	@Override
	public void run() {
		//read in file and chunk meta-data
		fileMap = new HashMap<String, RandomAccessFile>();
		chunkMap = new HashMap<String, ChunkInfo>();
		
		File dir = new File(Constants.filePath);
		String[] filenames = dir.list();
		for(int i = 0; i < filenames.length; i++) {
			try {
				//create new RAF and add to file map
				RandomAccessFile raf = new RandomAccessFile(Constants.filePath+filenames[i], "rw");
				fileMap.put(filenames[i], raf);
				//add chunks in file to file map
				while(raf.getFilePointer() < raf.length()) {
					String chunkHandle = Long.toString(raf.readLong());
					chunkHandles.add(chunkHandle);
					long pos = raf.getFilePointer();
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
			masterPort = config.readInt();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			Socket masterSocket = new Socket(masterIP, masterPort);
			masterWriteStream = new ObjectOutputStream(masterSocket.getOutputStream());
			masterReadStream = new ObjectInputStream(masterSocket.getInputStream());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//start client listener to listen for clients
		clientListener = new ClientListener();
		clientListener.start();
		
		//send heartbeats
		while(true) {
			sendHeartBeat(false, null);
			//TODO check if leases need renewals and send renewal request
			
			try {
				Thread.sleep(Constants.HeartBeatInterval);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
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
					break;
				case Constants.DeleteChunks:
					break;
				}
			}
		}
	}
	
	public class ClientListener extends Thread {
		//TODO check if client threads need to be kept in a vector
		@Override
		public void run() {
			int ServerPort = 0;
			
			try {
				clientServerSocket = new ServerSocket(ServerPort);
				ServerPort = clientServerSocket.getLocalPort();
			} catch(IOException e) {
				System.out.println("Error, failed to open a new socket to listen on.");
				e.printStackTrace();			
			}
			
			while(true) {
				//accept client connections and start new client thread
				try {
					Socket newClientSocket = clientServerSocket.accept();
					ClientThread clientThread = new ClientThread(newClientSocket);
					clientThread.start();
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
		
		public ClientThread(Socket socket) {
			this.socket = socket;
		}
		
		@Override
		public void run() {
			try {
				clientWriteStream = new ObjectOutputStream(socket.getOutputStream());
				clientReadStream = new ObjectInputStream(socket.getInputStream());
			} catch (IOException e) {
				System.out.println("Error, failed to create object I/O stream for client " + 
									socket.getInetAddress());
				e.printStackTrace();
			}
			
			while(true) {
				//process client requests
				int CMD = Client.ReadIntFromInputStream("ChunkServer.ClientThread:"+localhostIP, clientReadStream);
				try {
					switch(CMD) {
					case Constants.Ping:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						break;
					case Constants.ReadFirstRecord:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						break;
					case Constants.ReadLastRecord:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						break;
					case Constants.ReadNextRecord:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						break;
					case Constants.ReadPrevRecord:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						break;
					case Constants.AppendRecord:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
						break;
					case Constants.DataToWrite:
						clientWriteStream.writeInt(Constants.TRUE);
						clientWriteStream.flush();
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
	
	/* Stores metadata about a chunk, namely the file that
	 * it is stored in, its position, in bytes, in that file,
	 * and whether it currently owns the lease.
	 */
	public class ChunkInfo {
		private String filename;
		private long position;
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
