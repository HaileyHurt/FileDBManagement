package utilities;

public class Constants {
	public final static String filePath = "csci485/";	//or C:\\newfile.txt
	public final static String ConfigFile = "config";
	public final static int ChunkSize = 1024 * 1024; //1 MB chunk sizes
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer 
	
	//heartbeat message interval in millicseconds
	public static final int HeartBeatInterval = 2000;
	
	//Replies provided by the server
	public static final int FALSE = 0;
	public static final int TRUE = 1;
	
	//chunkserver to master
	public static final int HeartBeatNoLease = 2;
	public static final int HeartBeatWithLease = 3;
	
	//client to chunkserver
	public static final int Ping = 4;
	public static final int ReadFirstRecord = 5;
	public static final int ReadLastRecord = 6;
	public static final int ReadNextRecord = 7;
	public static final int ReadPrevRecord = 8;
	public static final int AppendRecord = 9;
	public static final int DataToWrite = 10;
	
	//master to chunkserver
	public static final int SendLease = 11;
	public static final int CreateChunk = 12;
	public static final int DeleteChunks = 13;		
	
    // Client to master commands
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
    
    //connection type identification
    public static final int IsClient = 29;
    public static final int IsChunkServer = 30;

	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
    
}
