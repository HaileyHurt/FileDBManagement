package com.master;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import com.client.Client;

import utilities.Constants;

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
	
	public ClientThread (DirectoryManager dirManager){
		this.dirManager = dirManager;
	}
	
	public void run() {
		ServerSocket commChanel = null;
		
		try {
			int portToClients = 0;
			commChanel = new ServerSocket(portToClients);
			portToClients = commChanel.getLocalPort();
			PrintWriter outWrite = new PrintWriter(new FileOutputStream(Constants.ConfigFile));
			System.out.println("Waiting for clients on the port:" + portToClients);
			outWrite.println(InetAddress.getLocalHost().getHostAddress());
			outWrite.println(portToClients);
			outWrite.close();
			this.port = portToClients;
		}
		catch (IOException ex) {
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
