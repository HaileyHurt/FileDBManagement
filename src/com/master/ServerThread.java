package com.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import com.client.Client;

public class ServerThread implements Runnable {
	private ChunkToServers chunksmap;
	private ArrayList<String> servers;
	private int port;
	
	public ServerThread (int port, ChunkToServers chunksmap, ArrayList<String> servers){
		this.port = port;
		this.chunksmap = chunksmap;
		this.servers = servers;
	}
	
	public void run() {
		ServerSocket commChanel = null;
		
		try {
			commChanel = new ServerSocket(port);
		} catch (IOException ex) {
			System.out.println("Error, failed to open a new socket to listen on.");
			ex.printStackTrace();
		}
		
		boolean done = false;
		Socket ServerConnection = null;
		while (!done){
			try {
				ServerConnection = commChanel.accept();
				Thread thr = new Thread(new ServerThreadCom(ServerConnection, chunksmap, servers));
				thr.start();				
			}
			catch (IOException ex){
				System.out.println("Server Disconnected");
			}
			finally {
				try {
					if (ServerConnection != null){
						ServerConnection.close();
					}
				}
				catch (IOException fex){
					System.out.println("Error (Master:ServertThread): Failed to close a valid connection.");
					fex.printStackTrace();
				}
			}
		}
	}
}
