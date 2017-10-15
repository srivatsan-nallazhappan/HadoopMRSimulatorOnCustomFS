package com.hadoopsimulator.input;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.UTF8;

// Note: UTF8 class is deprecated. hence using Hadoop Text class

public class CustomHadoopInputSplit extends org.apache.hadoop.mapreduce.InputSplit 
{
    private String    dataset; // for now it is the file name, a single one.
    private String[]  fullFilePaths;
    private long      start;
    private long      length;
    private String[]  hostnames;      // set only one host for now 
    private String    dataServerPort; // port for datanode
    
    public CustomHadoopInputSplit()
    { 
    	//
    };
    public CustomHadoopInputSplit(String dataset, String[] fullFilePaths ,long start,
      long length, String[] hosts, String dataServerPort )
    {
    	this.dataset         =  dataset;
    	this.start           =  start;
    	this.length          =  length;
    	this.hostnames       =  hosts;
    	this.fullFilePaths   =  fullFilePaths;
    	this.dataServerPort  =  dataServerPort; 
    }
    
    // InputSplit specification
    public String[] getLocations() throws IOException
    { 
    	if (hostnames == null)
    		return new String[] {};
    	return hostnames;
    }
	public long getLength() throws IOException { return length; }
    
	// writable interface implementation
	 public void write(DataOutput out) throws IOException
	 {
		Text.writeString(out, dataset);
		out.writeLong(start);
		out.writeLong(length);
		out.writeInt(hostnames.length);
		for ( int i = 0 ; i < hostnames.length;i++)
		   Text.writeString(out, hostnames[i]);
		Text.writeString(out, dataServerPort);
	 }
	 
     // read back the written data	 
	 public void readFields(DataInput in) throws IOException
	 {
		    dataset = Text.readString(in);
		    start = in.readLong();
		    length = in.readLong();
		    int hostl = in.readInt();
		    hostnames = new String[] {};
		    int index = 0;
		    for ( ; index < hostl ; index++ )
		      hostnames[index] = Text.readString(in);
		    dataServerPort = Text.readString(in);
		    return;	
	};
		  
       
    // toString Convenience method 
    public String toString() { return dataset + ":" + start + "+" + length + "@" + hostnames[0] + " : " +
    		                          dataServerPort; }
    
    public void dump(DataOutput out) throws IOException
    {
    }
    
    public String   getDataset() { return dataset;}
    public long     getStart()   { return start;}
    public String   getDataServerPort() 
    { 
    	return dataServerPort;
    }
    
    public String[] getFullFilePaths()
    {
    	if ( fullFilePaths == null )
    		return new String[] {};
    	return fullFilePaths;
    }
    
    // Debug -for now , dump to stdout, later we can look at SL4J or central logging
    
    public void dump()
    {
    	System.out.println("Dumping Split information ....");
    	System.out.println("-------------------------------");
    	System.out.println("Filename         :    " + dataset );
    	System.out.println("Start            :    " + start );
    	System.out.println("Length           :    " + length );
    	System.out.println("Full file paths  :    " + Arrays.toString(fullFilePaths));
    	System.out.println("Hostnames        :    " + Arrays.toString(hostnames));
    	System.out.println("EndPoints        :    " + dataServerPort );
    }
    
}
