package utilities;

public class IP
{
    private String address;
    private int port;
    
    public IP()
    {
        address = "";
        port = 0;
    }
    
    public int getPort()
    {
        return port;
    }
    public void setPort(int p)
    {
        port = p;
    }
    
    public String getAddress()
    {
        return address;
    }
    public void setAddress(String add)
    {
        address = add;
    }
}

