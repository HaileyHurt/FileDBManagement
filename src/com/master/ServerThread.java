package com.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import com.client.Client;

public class ServerThread implements Runnable {
	private int port;
	private DirectoryManager dirManager;
	
	public ServerThread (int port){
		this.port = port;
		dirManager = new DirectoryManager();
	}
	
	public ServerThread (int port, DirectoryManager dirManager){
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
				Socket ServerConnection = commChanel.accept();
				Thread thr = new Thread(new ServerThreadCom(ServerConnection, dirManager));
				thr.start();				
			}
			catch (IOException ex){
				System.out.println("Server Disconnected");
			}
		}
	}
}
