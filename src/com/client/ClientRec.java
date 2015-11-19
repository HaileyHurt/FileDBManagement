
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
    
    
    public static final int CREATE_DIR = 1;
    public static final int DELETE_DIR = 2;
    public static final int RENAME_DIR = 3;
    public static final int LIST_DIR = 4;
    public static final int CREATE_FILE = 5;
    public static final int DELETE_FILE = 6;
    public static final int OPEN_FILE = 7;
    public static final int CLOSE_FILE = 8;
    public static final int REGISTRATION_MESSAGE = 9;
    public static final int APPEND_RECORD = 10;
    public static final int DELETE_RECORD = 11;
    public static final int READ_FIRST_RECORD = 12;
    public static final int READ_LAST_RECORD = 13;
    public static final int READ_NEXT_RECORD = 14;
    public static final int READ_PREV_RECORD = 15;
    
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
	 * Example usage: 1. ReadFirstRecord(FH1, rec, rec1) 2. ReadNextRecord(FH1,
	 * rec1, rec, rec2) 3. ReadNextRecord(FH1, rec2, rec, rec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, byte[] payload, RID RecordID)
    {
		return null;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, rec, recn) 2. ReadPrevRecord(FH1,
	 * recn-1, rec, rec2) 3. ReadPrevRecord(FH1, recn-2, rec, rec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, byte[] payload, RID RecordID)
    {
		return null;
	}

}
