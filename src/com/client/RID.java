package com.client;

public class RID
{
    String chunkHandle;
    int offset;
    int recordSize;
    int index;
    boolean exists;
    
    public RID()
    {
        chunkHandle = "";
        offset = 0;
        recordSize = 0;
        exists = true;
    }
    
    public void setChunkHandle(String ch)
    {
        chunkHandle = ch;
    }
    
    public String getChunkHandle()
    {
        return exists ? chunkHandle : null;
    }
    
    public void setOffset(int os)
    {
        offset = os;
    }
    
    public int getOffset()
    {
        return exists ? offset : -1;
    }
    
    public void setIndex(int ind)
    {
        index = ind;
    }
    
    public int getIndex()
    {
        return index;
    }
    
    public void setRecordSize(int size)
    {
        recordSize = size;
    }
    
    public int getRecordSize()
    {
        return recordSize;
    }
    
    public void deleteRecord()
    {
        exists = false;
    }
    
    public boolean DoesExist()
    {
        return exists;
    }
}
