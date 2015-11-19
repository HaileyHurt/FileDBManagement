package com.master;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import com.client.Client;

import utilities.Constants;

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
	
	public ServerThread (DirectoryManager dirManager){
		this.dirManager = dirManager;
	}
	
	public void run() {
		ServerSocket commChanel = null;
		
		try {

			int portToServers = 0;
			commChanel = new ServerSocket(portToServers);
			portToServers = commChanel.getLocalPort();
			
			PrintWriter outWrite = new PrintWriter(new BufferedWriter(new FileWriter(Constants.ConfigFile, true)));
			outWrite.println(portToServers);
			outWrite.close();
						
			System.out.println("Waiting for servers on the port:" + portToServers);
			this.port = portToServers;
			
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
