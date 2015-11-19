package com.client;
import java.util.Vector;
import java.util.Map;
import utilities.IP;


public class FileHandle
{
    String fileName;
    Map<String, Vector<IP>> chunkToServerMap;
    
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
    
    public Vector<IP> getServersForChunk(String ch)
    {
        Vector<IP> vec = chunkToServerMap.get(ch);
        return vec;
    }
    
    public void addChunk(String ch, Vector<IP> servers)
    {
        chunkToServerMap.put(ch, servers);
    }
    
    public Vector<String> getChunkHandles()
    {
        Vector<String> chunkHandVec = new Vector<String>();
        for (Map.Entry<String, Vector<IP>> ch : chunkToServerMap.entrySet())
        {
            chunkHandVec.add(ch.getKey());
        }
        return chunkHandVec;
    }
    
    public Map<String, Vector<IP>> getChunkToServerMap()
    {
        return chunkToServerMap;
    }
    
    public void setChunkToServerMap(Map<String, Vector<IP>> map)
    {
        chunkToServerMap = map;
    }
    
    public Vector<IP> getIPsForChunk(String ch)
    {
        return chunkToServerMap.get(ch);
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
