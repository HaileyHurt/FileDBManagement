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
import java.util.Map;
import java.util.Vector;
import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

public class ClientFS {

	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail //Returned when a method fails
	}
    public Master master;
    public ChunkServer chunkserver;
    public Client client;
    public Socket clientSocket;
    public ObjectOutputStream masterOutput;
    public ObjectInputStream masterInput;
    
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
    
    public ClientFS()
    {
        try
        {
            master = new Master();
            clientSocket = new Socket("localhost", 2345);
            
            masterOutput = new ObjectOutputStream(clientSocket.getOutputStream());
            masterInput = new ObjectInputStream(clientSocket.getInputStream());
            client = new Client();
        }
        catch(Exception e)
        {
            System.out.println("Error initializing clientfs (ClientFS:43)");
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
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname)
    {
        FSReturnVals outcome;
        try
        {            
            byte[] srcBytes = src.getBytes();
            masterOutput.writeInt(CREATE_DIR);
            masterOutput.writeInt(srcBytes.length);
            masterOutput.write(srcBytes);
            
            byte[] dirBytes = dirname.getBytes();
            masterOutput.writeInt(dirBytes.length);
            masterOutput.write(dirBytes);
            
            masterOutput.flush();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Error creating directory (ClientFS:78)");
        }
		return outcome;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname)
    {
        FSReturnVals outcome;
		try
        {
            masterOutput.writeInt(DELETE_DIR);
            
            byte[] srcBytes = src.getBytes();
            masterOutput.writeInt(srcBytes.length);
            masterOutput.write(srcBytes);
            
            byte[] dirBytes = dirname.getBytes();
            masterOutput.writeInt(dirBytes.length);
            masterOutput.write(dirBytes);
            
            masterOutput.flush();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Error deleting directory (FlientFS:103)");
        }
        return outcome;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName)
    {
        FSReturnVals outcome;
		try
        {
            masterOutput.writeInt(RENAME_DIR);
            
            byte[] srcBytes = src.getBytes();
            masterOutput.writeInt(srcBytes.length);
            masterOutput.write(srcBytes);
            
            byte[] newBytes = NewName.getBytes();
            masterOutput.writeInt(newBytes.length);
            masterOutput.write(newBytes);
            
            masterOutput.flush();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Error renaming directory (ClientFS:129)");
        }
        return outcome;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt)
    {
        FSReturnVals outcome;
        try
        {
            masterOutput.writeInt(LIST_DIR);
            
            byte[] tgtBytes = tgt.getBytes();
            masterOutput.writeInt(tgtBytes.length);
            masterOutput.write(tgtBytes);
            
            masterOutput.flush();
            
            int dirSize = masterInput.readInt();
            String[] contents = new String[dirSize];
            for (int i = 0; i < dirSize; i++)
            {
                int sz = masterInput.readInt();
                byte[] bytes = new byte[sz];
                masterInput.read(bytes, 0, sz);
                String temp = new String(bytes);
                contents[i] = temp;
            }
            return contents;
        }
        catch (Exception e)
        {
            System.out.println("Error listing directories (ClientFS:151)");
        }
        return null;
        /*
         */
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename)
    {
        FSReturnVals outcome;
		try
        {
            masterOutput.writeInt(CREATE_FILE);
            
            byte[] tgtBytes = tgtdir.getBytes();
            masterOutput.writeInt(tgtBytes.length);
            masterOutput.write(tgtBytes);
            
            byte[] fileBytes = filename.getBytes();
            masterOutput.writeInt(fileBytes.length);
            masterOutput.write(fileBytes);
            
            masterOutput.flush();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Error creating file (ClientFS:178)");
        }
        return outcome;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename)
    {
        FSReturnVals outcome;
		try
        {
            masterOutput.writeInt(DELETE_FILE);
            
            byte[] tgtBytes = tgtdir.getBytes();
            masterOutput.writeInt(tgtBytes.length);
            masterOutput.write(tgtBytes);
            
            byte[] fileBytes = filename.getBytes();
            masterOutput.writeInt(fileBytes.length);
            masterOutput.write(fileBytes);
            
            masterOutput.flush();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Error creating file (ClientFS:203)");
        }
        return outcome;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx")
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh)
    {
        FSReturnVals outcome;
		try
        {
            masterOutput.writeInt(OPEN_FILE);
            
            byte[] fileBytes = FilePath.getBytes();
            masterOutput.writeInt(fileBytes.length);
            masterOutput.write(fileBytes);
            
            masterOutput.flush();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Error opening file (ClientFS:228)");
        }
        return outcome;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh)
    {
        FSReturnVals outcome;
		try
        {
            masterOutput.writeInt(CLOSE_FILE);
            masterOutput.writeObject(ofh);
            masterOutput.flush();
            
            int i = masterInput.readInt();
            outcome = intToFSReturnVal(i);
        }
        catch (Exception e)
        {
            outcome = FSReturnVals.Fail;
            System.out.println("Error opening file (ClientFS:228)");
        }
        return outcome;
	}

}