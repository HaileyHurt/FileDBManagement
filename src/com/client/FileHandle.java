package com.client;
import java.util.Vector;
import java.util.Map;


public class FileHandle
{
    String fileName;
    Map<String, Vector<String>> chunkToServerMap;
    boolean opened;
    
    public FileHandle()
    {
        fileName = "";
        opened = false;
    }
    
    public FileHandle(String fn)
    {
        fileName = fn;
        opened = false;
    }
    
    public void setFileName(String fn)
    {
        fileName = fn;
    }
    
    public String getFileName()
    {
        return fileName;
    }
    
    public void addChunk(String ch, Vector<String> locs)
    {
        chunkHandles.put(ch);
        chunkToServerMap.put(ch, locs);
    }
    
    public Vector<String> getChunkHandles()
    {
        Vector chunkHandVec;
        for (Map.Entry<String, Vector<String> ch : chunkToServerMap.entrySet())
        {
            chunkHandVec.addEntry(ch.getKey());
        }
        return chunkHandVec;
    }
    
    public Map<String, Vector<String>> getChunkToServerMap()
    {
        return chunkToServerMap;
    }
    
    public boolean isOpen()
    {
        return opened;
    }
    
    public void open()
    {
        opened = true;
    }
    public void close()
    {
        opened = false;
    }
}
