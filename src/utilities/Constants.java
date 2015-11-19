package utilities;

public class Constants {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public final static String ConfigFile = "config";
	public final static int ChunkSize = 1024 * 1024; //1 MB chunk sizes
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer  
	
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
	
	//heartbeat message interval in millicseconds
	public static final int HeartBeatInterval = 2000;
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;


}
