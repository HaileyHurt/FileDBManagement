package com.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import com.client.Client;

public class ClientThread implements Runnable {
	private DirectoryManager dirManager;
	private int port;
	
	public ClientThread (int port){
		this.port = port;
		dirManager = new DirectoryManager();
	}
	
	public ClientThread (int port, DirectoryManager dirManager){
		this.port = port;
		this.dirManager = dirManager;
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
		while (!done){
			try {
				Socket ClientConnection = commChanel.accept();
				System.out.println ("Connection with: " + ClientConnection.getLocalAddress());
				
				Thread th1 = new Thread(new ClientThreadCom(ClientConnection, dirManager));
				th1.start();
			}
			catch (Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
}
