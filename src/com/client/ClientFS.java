package com.client;

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
    
    public ClientFS()
    {
        master = new Master();
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
		return master.CreateDir(src, dirname);
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
		return master.DeleteDir(src, dirname);
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
		return master.RenameDir(src, NewName);
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
		return master.ListDir(tgt);
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
		return master.CreateFile(tgtdir, filename);
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
		return master.DeleteFile(tgtdir, filename);
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
		return master.OpenFile(FilePath, ofh);
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh)
    {
		return master.CloseFile(ofh);
	}

}


public class Master
{
    Map<String, Vector<String>> namespace;
    Map<String, Vector<String>> fileChunkMap;
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
        if (!namespace.containsKey(src))
        {
            return FSReturnVals.SrcDirNotExistent
        }
        
        if (namespace.containsKey(src+"/"+dirname))
        {
            return FSReturnVals.DestDirExists;
        }
        
        namespace[src].addElement(src+"/"+dirname);
        Vector emptyVec = new Vector<String>();
        namespace.put(src+"/"+dirname, emptyVec);
        
        if (namespace[src].containsKey(src+"/"+dirname))
        {
            return FSReturnVals.Success;
        }
        else
        {
            FSReturnVals.Fail;
        }
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
        if (!namespace.containsKey(src))
        {
            return FSReturnVals.SrcDirNotExistent;
        }
        /*
        if (namespace.containsKey(src+"/"+dirname))
        {
            return FSReturnVals.DestDirExists;
        }
        */
        if (namespace[src+"/"+dirname].size() != 0))
        {
            return FSReturnVals.DirNotEmpty;
        }
        
        namespace[src].remove(src+"/"+dirname);
        
        if(namespace[src].containsKey(src+"/"+dirname))
        {
            return FSReturnVals.Fail;
        }
        else
        {
            return FSReturnVals.Success;
        }
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
        if (!namespace.containsKey(src))
        {
            return FSReturnVals.SrcDirNotExistent;
        }
        
        if (namespace.containsKey(src+"/"+NewName))
        {
            return FSReturnVals.DestDirExists;
        }
        
        String parentString = src.substring(0, src.lastIndexOf("/"));
        namespace[parentString].remove(src);
        namespace[parentString].addElement(src+"/"+NewName);
        
        Vector<String> todoList = new Vector<String>;
        Vector<String> childVec = new Vector<String>;
        Vector<String> doneList = new Vector<String>;
        
        todoList.put(src+"/");
        for (int i = 0; i < numChildren; i++)
        {
            childVec = namespace.remove(todoList[i]);
            
            for (int j = 0; j < childVec.size(); j++)
            {
                todoList.put(childVec[j]+"/");
                String newString = childVec[y].replace(src, NewName);
                doneList.put(newString);
            }
        }
        return FSReturnVals.Success;
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
        return null;
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
        if (!namespace.containsKey(tgtdir))
        {
            return FSReturnVals.SrcDirNotExistent;
        }
        
        File f = new File(tgtdir, filename);
        if (f.exists())
        {
            return FSReturnVals.FileExists;
        }
        
        namespace[tgtdir].put(tgtdir+"/"+filename);
        Vector<String> v = new Vector<String>;
        fileChunkMap.put(filename, v);
        
        if (namespace[tgtdir].containsKey(tgtdir+"/"+filename))
        {
            return FSReturnVals.Success;
        }
        else
        {
            return FSReturnVals.Fail;
        }
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
        if (!namespace.containsKey(tgtdir))
        {
            return FSReturnVals.SrcDirNotExistent;
        }
        
        if (!namespace[tgtdir].containsKey(tgtdir+"/"+filename))
        {
            return FSReturnVals.FileDoesNotExist;
        }
        
        fileChunkMap.remove(tgtdir);
        namespace[tgtdir].remove(tgtdir+"/"+filename);
        
        File f = new File(tgtdir, filename);
        if (f.exists())
        {
            return FSReturnVals.Fail;
        }
        else
        {
            return FSReturnVals.Success;
        }
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
        if(!fileChunkMap.containsKey(FilePath))
        {
            return FSReturnVals.FileDoesNotExist;
        }
        
        for (int i = 0; i < fileChunkMap[FilePath].size(); i++)
        {
            ofh = new FileHandle();
            ofh.setFileName(FilePath);
            ofh.addChunk(fileChunkMap[FilePath]);
            ofh.open();
        }
        return FSReturnVals.Success;
    }
    
    /**
     * Closes the specified file handle Returns BadHandle if ofh is invalid
     *
     * Example usage: CloseFile(FH1)
     */
    public FSReturnVals CloseFile(FileHandle ofh)
    {
        if(!fileChunkMap.containsKey(ofh.getName()))
        {
            return FSReturnVals.BadHandle;
        }
        
        ofh.close();
        return FSReturnVals.Success;
    }
}