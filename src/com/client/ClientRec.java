
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
        try
        {
            masteroutput.writeInt(APPEND_RECORD);
            masteroutput.writeObject(ofh);
            masteroutput.flush();
            masteroutput.writeObject(RecordID);
            masteroutput.flush();
            masteroutput.writeInt(payload.length);
            masteroutput.flush();
            
            
            FileHandle fh = (FileHandle) masterInput.readObject();
            ofh.getFileName = fh.getFileName;
            
            RID rid = (RID) masterInput.readObject();
            RecordID.chunkhandle = rid.chunkhandle;
            RecordID.byteoffset = rid.byteoffset;
            RecordID.size = rid.size;
            
            outcome = masterInput.readUTF();
            
        }
        catch (Exception e)
        {
            System.out.println("Unable to append records (ClientREC:65)");
        }
        
        if (outcome == FSReturnVals.Success)
        {
            ClientFS.client.writeChunk(RecordID.chunkhandle, payload, RecordID.byteoffset);
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
            
            outcome = masterInput.readUTF();
            
        }
        catch (Exception e)
        {
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
            
            outcome = masterInput.readUTF();
            
        }
        catch (Exception e)
        {
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
        FSReturnVals outcome = null;
        
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
            
            outcome = masterInput.readUTF();
            
        }
        catch (Exception e)
        {
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
