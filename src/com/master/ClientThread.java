package com.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ClientThread implements Runnable {
	private DirectoryTree directory;
	private int port;
	
	public ClientThread (int port){
		this.port = port;
		directory = new DirectoryTree();
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
		Socket ClientConnection = null;
		while (!done){
			try {
				ClientConnection = commChanel.accept();
				Thread thr = new Thread(new ClientThreadCom(ClientConnection, directory));
				thr.start();				
			}
			catch (IOException ex){
				System.out.println("Client Disconnected");
			}
			finally {
				try {
					if (ClientConnection != null){
						ClientConnection.close();
					}
				}
				catch (IOException fex){
					System.out.println("Error (Master:ClientThread): Failed to close a valid connection.");
					fex.printStackTrace();
				}
			}
		}
	}
	
}
