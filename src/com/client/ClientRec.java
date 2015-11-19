
package com.client;

import com.client.Client;
import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.client.Client;
import com.master.*;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;

import utilities.Constants;
import utilities.IP;
import java.util.Map;
import java.util.Vector;
import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;
import com.client.RID;
import com.client.FileHandle;
import com.client.TinyRec;

public class ClientRec {

	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
     
	 */
    
    public final static int ChunkSize = 4 * 1024;
    
    
    public static final int CREATE_DIR = 14;
    public static final int DELETE_DIR = 15;
    public static final int RENAME_DIR = 16;
    public static final int LIST_DIR = 17;
    public static final int CREATE_FILE = 18;
    public static final int DELETE_FILE = 19;
    public static final int OPEN_FILE = 20;
    public static final int CLOSE_FILE = 21;
    public static final int REGISTRATION_MESSAGE = 22;
    public static final int APPEND_RECORD = 23;
    public static final int DELETE_RECORD = 24;
    public static final int READ_FIRST_RECORD = 25;
    public static final int READ_LAST_RECORD = 26;
    public static final int READ_NEXT_RECORD = 27;
    public static final int READ_PREV_RECORD = 28;
    
    public Master master;
    public ChunkServer chunkserver;
    public Client client;
    public Socket clientSocket;
    public ObjectOutputStream masterOutput;
    public ObjectInputStream masterInput;
    
    public ClientRec()
    {
        try
        {
            master = new Master();
            clientSocket = new Socket("localhost", 2345);
            
            masterOutput = new ObjectOutputStream(clientSocket.getOutputStream());
            masterInput = new ObjectInputStream(clientSocket.getInputStream());
            client = new Client();
            
        }
        catch (Exception e)
        {
            System.out.println("Error initializing clientrec (ClientRec:60)");
        }
    }
    
    public FSReturnVals intToFSReturnVal(int i)
    {
        switch (i)
        {
            case 0:
                return FSReturnVals.DirExists;
            case 1:
                return FSReturnVals.DirNotEmpty;
            case 2:
                return FSReturnVals.SrcDirNotExistent;
            case 3:
                return FSReturnVals.DestDirExists;
            case 4:
                return FSReturnVals.FileExists;
            case 5:
                return FSReturnVals.FileDoesNotExist;
            case 6:
                return FSReturnVals.BadHandle;
            case 7:
                return FSReturnVals.RecordTooLong;
            case 8:
                return FSReturnVals.BadRecID;
            case 9:
                return FSReturnVals.RecDoesNotExist;
            case 10:
                return FSReturnVals.NotImplemented;
            case 11:
                return FSReturnVals.Success;
            case 12:
                return FSReturnVals.Fail;
            default:
                return FSReturnVals.Fail;
                
        }
    }
    /**
     * Appends a record to the open file as specified by ofh Returns BadHandle
     * if ofh is invalid Returns BadRecID if the specified RID is not null
     * Returns RecordTooLong if the size of payload exceeds chunksize RID is
     * null if AppendRecord fails
     *
     * Example usage: AppendRecord(FH1, obama, RecID1)
     */
    public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID)
    {
        FSReturnVals outcome = FSReturnVals.Fail;
        // should set ip address, port number
        Vector<IP> servers = ofh.getServersForChunk(RecordID.getChunkHandle());
        int numServers = servers.size();
        if (ofh == null)
        {
            return FSReturnVals.BadHandle;
        }
        if (RecordID != null)
        {
            return FSReturnVals.BadRecID;
        }
        if (payload.length > ChunkSize)
        {
            return FSReturnVals.RecordTooLong;
        }
        for(int i = 0; i < numServers; i++)
        {
            IP ip = new IP();
            ip.setPort(servers.elementAt(i).getPort());
            ip.setAddress(servers.elementAt(i).getAddress());
            try
            {
                Socket sock = new Socket(ip.getAddress(), ip.getPort());
                ObjectOutputStream serverOutput = new ObjectOutputStream(sock.getOutputStream());
                serverOutput.flush();
                ObjectInputStream serverInput = new ObjectInputStream(sock.getInputStream());
                serverOutput.writeInt(Constants.IsClient);
                
                serverOutput.writeInt(Constants.APPEND_RECORD);
                serverOutput.writeInt(RecordID.getChunkHandle().length());
                serverOutput.writeBytes(RecordID.getChunkHandle());
                serverOutput.flush();
                serverOutput.writeInt(RecordID.index);
                serverOutput.writeInt(numServers);
                
                int response = serverInput.readInt();
                if (response == Constants.FALSE)
                {
                    i--;
                    continue;
                }
                else
                {
                    for(int j = 0; j < numServers; j++)
                    {
                        byte[] ipadd = ip.getAddress().getBytes();
                        serverOutput.writeInt(ipadd.length);
                        serverOutput.flush();
                        serverOutput.write(ipadd);
                        serverOutput.writeInt(ip.getPort());
                        serverOutput.flush();
                    }
                    
                    serverOutput.writeInt(payload.length);
                    serverOutput.write(payload);
                }
                
                //int type = serverOutput.readInt();
                //int command = serverOutput.readInt();
                //if(type == Constants.IsServer && command = Constants.APPEND_RECORD)
                //{
                    int ch = serverInput.readInt();
                    int ind = serverInput.readInt();
                    int ipaddlen = serverInput.readInt();
                byte[] ipadd = new byte[ipaddlen];
                serverInput.read(ipadd, 0, ipaddlen);
                    String ipstr = new String(ipadd, "UTF-8");
                //}
                
                int j = serverInput.readInt();
                outcome = intToFSReturnVal(j);

            }
            catch (Exception e)
            {
                outcome = FSReturnVals.Fail;
                System.out.println("Unable to append records (ClientREC:65)");
            }
        }

        return outcome;
    }

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID)
    {
        if (ofh == null)
        {
            return FSReturnVals.BadHandle;
        }
        if (RecordID == null)
        {
            return FSReturnVals.BadRecID;
        }
        
        FSReturnVals outcome = FSReturnVals.Fail;
        Vector<IP> servers = ofh.getServersForChunk(RecordID.getChunkHandle());
        int numServers = servers.size();
        for (int i = 0; i < numServers; i++)
        {
            IP ip = new IP();
            ip.setPort(servers.elementAt(i).getPort());
            ip.setAddress(servers.elementAt(i).getAddress());
            try
            {
                Socket sock = new Socket(ip.getAddress(), ip.getPort());
                ObjectOutputStream serverOutput = new ObjectOutputStream(sock.getOutputStream());
                serverOutput.flush();
                ObjectInputStream serverInput = new ObjectInputStream(sock.getInputStream());
                serverOutput.writeInt(Constants.IsClient);
               
                serverOutput.writeInt(Constants.DELETE_RECORD);
                serverOutput.writeInt(RecordID.getChunkHandle().length());
                serverOutput.writeBytes(RecordID.getChunkHandle());
                serverOutput.flush();
                serverOutput.writeInt(RecordID.index);
                serverOutput.writeInt(numServers);
                
                int response = serverInput.readInt();
                if (response == Constants.FALSE)
                {
                    i--;
                    continue;
                }
                
                int j = serverInput.readInt();
                outcome = intToFSReturnVal(j);
                
            }
            catch (Exception e)
            {
                outcome = FSReturnVals.Fail;
                System.out.println("Unable to append records (ClientREC:65)");
            }

        }
        
        return outcome;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, rec, recid)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec)
    {
        FSReturnVals outcome = FSReturnVals.Fail;
        // should set ip address, port number
        Vector<IP> servers = ofh.getServersForChunk(rec.getRID().getChunkHandle());
        int numServers = servers.size();
        if (ofh == null)
        {
            return FSReturnVals.BadHandle;
        }
        if (rec == null)
        {
            return FSReturnVals.BadRecID;
        }
        for(int i = 0; i < numServers; i++)
        {
            IP ip = new IP();
            ip.setPort(servers.elementAt(i).getPort());
            ip.setAddress(servers.elementAt(i).getAddress());
            try
            {
                Socket sock = new Socket(ip.getAddress(), ip.getPort());
                ObjectOutputStream serverOutput = new ObjectOutputStream(sock.getOutputStream());
                serverOutput.flush();
                ObjectInputStream serverInput = new ObjectInputStream(sock.getInputStream());
                serverOutput.writeInt(Constants.IsClient);
                
                serverOutput.writeInt(Constants.READ_FIRST_RECORD);
                serverOutput.writeInt(rec.getRID().getChunkHandle().length());
                serverOutput.writeBytes(rec.getRID().getChunkHandle());
                serverOutput.flush();
                
                int response = serverInput.readInt();
                if (response == Constants.FALSE)
                {
                    i--;
                    continue;
                }
                
                //int type = serverOutput.readInt();
                //int command = serverOutput.readInt();
                //if(type == Constants.IsServer && command = Constants.READ_FIRST_RECORD)
                //{
                    int sz = serverInput.readInt();
                byte[] data = new byte[sz];
                serverInput.read(data, 0, sz);
                    String datastr = new String(data, "UTF-8");
                    if(data.length > 0)
                    {
                        int j = serverInput.readInt();
                        outcome = intToFSReturnVal(j);
                        rec.setPayload(data);
                        break;
                    }
                    else
                    {
                        i--;
                        continue;
                    }
                //}
                //else
                //{
                //   int j = serverInput.readInt();
                //    outcome = intToFSReturnVal(j)
                //}
            }
            catch (Exception e)
            {
                outcome = FSReturnVals.Fail;
                System.out.println("Unable to append records (ClientREC:65)");
            }
        }
        
        return outcome;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, rec, recid)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, byte[] payload, RID RecordID)
    {
        FSReturnVals outcome = FSReturnVals.Fail;
        // should set ip address, port number
        Vector<IP> servers = ofh.getServersForChunk(RecordID.getChunkHandle());
        int numServers = servers.size();
        if (ofh == null)
        {
            return FSReturnVals.BadHandle;
        }
        if (RecordID == null)
        {
            return FSReturnVals.BadRecID;
        }
        for(int i = 0; i < numServers; i++)
        {
            IP ip = new IP();
            ip.setPort(servers.elementAt(i).getPort());
            ip.setAddress(servers.elementAt(i).getAddress());
            try
            {
                Socket sock = new Socket(ip.getAddress(), ip.getPort());
                ObjectOutputStream serverOutput = new ObjectOutputStream(sock.getOutputStream());
                serverOutput.flush();
                ObjectInputStream serverInput = new ObjectInputStream(sock.getInputStream());
                serverOutput.writeInt(Constants.IsClient);
                
                serverOutput.writeInt(Constants.READ_LAST_RECORD);
                serverOutput.writeInt(RecordID.getChunkHandle().length());
                serverOutput.writeBytes(RecordID.getChunkHandle());
                serverOutput.flush();
                
                int response = serverInput.readInt();
                if (response == Constants.FALSE)
                {
                    i--;
                    continue;
                }
                
                //int type = serverOutput.readInt();
                //int command = serverOutput.readInt();
                //if(type == Constants.IsServer && command = Constants.READ_LAST_RECORD)
                //{
                    int sz = serverInput.readInt();
                byte[] data = new byte[sz];
                serverInput.read(data, 0, sz);
                    String datastr = new String(data, "UTF-8");
                    if(data.length > 0)
                    {
                        int j = serverInput.readInt();
                        outcome = intToFSReturnVal(j);
                        for (int k = 0; k < data.length; k++)
                        {
                            payload[k] = data[k];
                        }
                        break;
                    }
                    else
                    {
                        i--;
                        continue;
                    }
                //}
                //else
                //{
                //    int j = serverInput.readInt();
                //    outcome = intToFSReturnVal(j)
                //}
            }
            catch (Exception e)
            {
                outcome = FSReturnVals.Fail;
                System.out.println("Unable to append records (ClientREC:65)");
            }
        }
        
        return outcome;
	}

    /**
     * Reads the next record after the specified pivot of the file specified by
     * ofh into payload Returns BadHandle if ofh is invalid Returns
     * RecDoesNotExist if the file is empty or pivot is invalid
     *
     * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
     * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
     */
    public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec)
    {
        FSReturnVals outcome = FSReturnVals.Fail;
        // should set ip address, port number
        Vector<IP> servers = ofh.getServersForChunk(pivot.getChunkHandle());
        int numServers = servers.size();
        if (ofh == null)
        {
            return FSReturnVals.BadHandle;
        }
        if (rec == null)
        {
            return FSReturnVals.RecDoesNotExist;
        }
        for(int i = 0; i < numServers; i++)
        {
            IP ip = new IP();
            ip.setPort(servers.elementAt(i).getPort());
            ip.setAddress(servers.elementAt(i).getAddress());
            try
            {
                Socket sock = new Socket(ip.getAddress(), ip.getPort());
                ObjectOutputStream serverOutput = new ObjectOutputStream(sock.getOutputStream());
                serverOutput.flush();
                ObjectInputStream serverInput = new ObjectInputStream(sock.getInputStream());
                serverOutput.writeInt(Constants.IsClient);
                
                serverOutput.writeInt(Constants.READ_NEXT_RECORD);
                serverOutput.writeInt(pivot.getChunkHandle().length());
                serverOutput.writeBytes(pivot.getChunkHandle());
                serverOutput.flush();
                serverOutput.writeInt(pivot.index);
                
                int response = serverInput.readInt();
                if (response == Constants.FALSE)
                {
                    i--;
                    continue;
                }
                else if (response == Constants.NOT_IN_CHUNK)
                {
                    i--;
                    continue;
                }
                
                //int type = serverOutput.readInt();
                //int command = serverOutput.readInt();
                //if(type == Constants.IsServer && command = Constants.READ_NEXT_RECORD)
                //{
                    int sz = serverInput.readInt();
                byte[] data = new byte[sz];
                serverInput.read(data, 0, sz);
                    String datastr = new String(data, "UTF-8");
                    if(data.length > 0)
                    {
                        int j = serverInput.readInt();
                        outcome = intToFSReturnVal(j);
                        rec.setPayload(data);
                        break;
                    }
                    else
                    {
                        i--;
                        continue;
                    }
                //}
                //else
                //{
                //    int j = serverInput.readInt();
                //    outcome = intToFSReturnVal(j)
                //}
                
            }
            catch (Exception e)
            {
                outcome = FSReturnVals.Fail;
                System.out.println("Unable to append records (ClientREC:65)");
            }
        }
        
        return outcome;
    }
    
    /**
     * Reads the previous record after the specified pivot of the file specified
     * by ofh into payload Returns BadHandle if ofh is invalid Returns
     * RecDoesNotExist if the file is empty or pivot is invalid
     *
     * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
     * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
     */
    public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec)
    {
        FSReturnVals outcome = FSReturnVals.Fail;
        // should set ip address, port number
        Vector<IP> servers = ofh.getServersForChunk(pivot.getChunkHandle());
        int numServers = servers.size();
        if (ofh == null)
        {
            return FSReturnVals.BadHandle;
        }
        if (rec == null)
        {
            return FSReturnVals.RecDoesNotExist;
        }
        for(int i = 0; i < numServers; i++)
        {
            IP ip = new IP();
            ip.setPort(servers.elementAt(i).getPort());
            ip.setAddress(servers.elementAt(i).getAddress());
            try
            {
                Socket sock = new Socket(ip.getAddress(), ip.getPort());
                ObjectOutputStream serverOutput = new ObjectOutputStream(sock.getOutputStream());
                serverOutput.flush();
                ObjectInputStream serverInput = new ObjectInputStream(sock.getInputStream());
                serverOutput.writeInt(Constants.IsClient);
                
                serverOutput.writeInt(Constants.READ_PREV_RECORD);
                serverOutput.writeInt(pivot.getChunkHandle().length());
                serverOutput.writeBytes(pivot.getChunkHandle());
                serverOutput.flush();
                serverOutput.writeInt(pivot.index);
                
                int response = serverInput.readInt();
                if (response == Constants.FALSE)
                {
                    i--;
                    continue;
                }
                else if (response == Constants.NOT_IN_CHUNK)
                {
                    i--;
                    continue;
                }
                
                //int type = serverOutput.readInt();
                //int command = serverOutput.readInt();
                //if(type == Constants.IsServer && command = Constants.READ_NEXT_RECORD)
                //{
                int sz = serverInput.readInt();
                byte[] data = new byte[sz];
                serverInput.read(data, 0, sz);
                String datastr = new String(data, "UTF-8");
                if(data.length > 0)
                {
                    int j = serverInput.readInt();
                    outcome = intToFSReturnVal(j);
                    rec.setPayload(data);
                    break;
                }
                else
                {
                    i--;
                    continue;
                }
                //}
                //else
                //{
                //    int j = serverInput.readInt();
                //    outcome = intToFSReturnVal(j)
                //}
                
            }
            catch (Exception e)
            {
                outcome = FSReturnVals.Fail;
                System.out.println("Unable to append records (ClientREC:65)");
            }
        }
        
        return outcome;
    }
    
}
