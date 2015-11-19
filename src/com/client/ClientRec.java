
package com.client;

import com.client.ClientFS.FSReturnVals;

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

        catch
        {
            System.out.println("Error initializing clientrec (ClientRec:60)");
        }
    }
    
    public FSReturnVals intToFSReturnVal(int i)
    {
        switch (i)
        {
            case 0:
                return DirExists;
                break;
            case 1:
                return DirNotEmpty;
                break;
            case 2:
                return SrcDirNotExistent;
                break;
            case 3:
                return DestDirExists;
                break;
            case 4:
                return FileExists;
                break;
            case 5:
                return FileDoesNotExist;
                break;
            case 6:
                return BadHandle;
                break;
            case 7:
                return RecordTooLong;
                break;
            case 8:
                return BadRecID;
                break;
            case 9:
                return RecDoesNotExist;
                break;
            case 10:
                return NotImplemented;
                break;
            case 11:
                return Success;
                break;
            case 12:
                return Fail;
                break;
                
                
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
        FSReturnVals outcome;
        IP ip;  // should set ip address, port number
        Vector<String> servers = ofh.getChunkHandles();
        int numServers = servers.size();
        
        for(int i = 0; i < numServers; i++)
        {
            try
            {   
                RID rid = (RID) masterInput.readObject(); 
                RecordID.chunkhandle = rid.chunkhandle;
                RecordID.byteoffset = rid.byteoffset;
                RecordID.size = rid.size;
                
                masterOutput.writeInt(Constants.APPEND_RECORD); 
                masterOutput.writeInt(rid.chunkhandle.length);
                masterOutput.writeInt(rid.chunkhandle);
                masterOutput.flush(); 
                masterOutput.writeInt(rid.index); 
                masterOutput.writeInt(numServers);
                byte[] ipadd = ip.getAddress().getBytes();
                masterOutput.writeInt(ipadd.length);
                masterOutput.flush();
                masterOutput.write(ipadd);
                masterOutput.writeInt(ip.getPort()); 
                masterOutput.flush(); 
                
                FileHandle fh = (FileHandle) masterInput.readObject();
                ofh.getFileName = fh.getFileName;
                
                int j = masterInput.readInt();
                outcome = intToFSReturnVal(j)

            }
            catch (Exception e)
            {
                outcome = FSReturnVals.Fail;
                System.out.println("Unable to append records (ClientREC:65)");
            }
            
            if(outcome == Constants.TRUE)
            {
                ClientFS.client.writeChunk(RecordID.chunkhandle, payload, RecordID.byteoffset);
                outcome = FSReturnVals.Success; 
            }
            else if(outcome == Constants.FALSE)
            {
                masterOutput.writeInt(Constants.DataToWrite);
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
        FSReturnVals outcome;
        try
        {
            masteroutput.writeInt(DELETE_RECORD);
            masteroutput.writeObject(ofh);
            masteroutput.flush();
            masteroutput.writeObject(RecordID);
            masteroutput.flush();
            
            FileHandle fh = (FileHandle) masterInput.readObject();
            ofh.setFileName = fh.getFileName;
            if (fh.isOpen())
            {
                ofh.open();
            }
            
            RID rid = (RID) masterInput.readObject();
            RecordID.setChunkHandle() = rid.getChunkHandle();
            RecordID.setOffset() = rid.getOffset();
            RecordID.setRecordSize() = rid.getRecordSize();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Unable to delete record (ClientRED:115)");
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
        FSReturnVals outcome;
        
        try
        {
            masteroutput.writeInt(READ_FIRST_RECORD);
            masteroutput.writeObject(ofh);
            masteroutput.flush();
            
            FileHandle fh = (FileHandle) masterInput.readObject();
            ofh.getFileName = fh.setFileName;
            if (tempFH.isOpen())
            {
                ofh.open();
            }
            
            RID rid = (RID) masterInput.readObject();
            rec.setRID(rid);
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Unable to read first record (ClientREC:150)");
        }
        
        if (converted != FSReturnVals.Success)
        {
            rec.setRID(null);
        }
        else
        {
            byte[] data = ClientFS.client.readChunk(rec.getRID().getChunkHandle, rec.getRID().getOffset, rec.getRID().getRecordSize);
            rec.setPayload(data);
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
        FSReturnVals outcome;
        
        try
        {
            masteroutput.writeInt(READ_LAST_RECORD);
            masteroutput.writeObject(ofh);
            masteroutput.flush();
            
            FileHandle fh = (FileHandle) masterInput.readObject();
            ofh.setFileName = fh.getFileName;
            if (fh.isOpen())
            {
                ofh.open();
            }
            
            RID rid = (RID) masterInput.readObject();
            rec.setRID(rid);
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Unable to read last record (ClientREC:194)");
        }
        
        if (converted != FSReturnVals.Success)
        {
            rec.setRID(null);
        }
        else
        {
            byte[] data = ClientFS.client.readChunk(rec.getRID().getChunkHandle, rec.getRID().getOffset, rec.getRID().getRecordSize);
            rec.setPayload(data);
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
    public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
        return null;
    }
    
    /**
     * Reads the previous record after the specified pivot of the file specified
     * by ofh into payload Returns BadHandle if ofh is invalid Returns
     * RecDoesNotExist if the file is empty or pivot is invalid
     *
     * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
     * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
     */
    public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
        return null;
    }
    
}
