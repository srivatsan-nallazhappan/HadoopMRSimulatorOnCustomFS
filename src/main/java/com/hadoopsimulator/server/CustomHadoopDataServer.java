package com.hadoopsimulator.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.hadoopsimulator.property.PropertyHandler;


public class CustomHadoopDataServer {
	
	private static String basePath = "";
	private static String clientPort = "";
	public class WorkerRunnable implements Runnable{

	    protected Socket clientsocket = null;

	    public WorkerRunnable(Socket clientSocket) {
	        this.clientsocket = clientSocket;
	    }

		public void run() {
			// TODO Auto-generated method stub
            try {
          	    DataInputStream in = new DataInputStream(clientsocket.getInputStream());
                String jsonreqstr = in.readUTF();
                JSONObject jsonobj = new JSONObject(jsonreqstr);

                JSONArray jsonarr = jsonobj.getJSONArray("pathLocations");
                String filelocation = jsonarr.getString(0);
                System.out.println("DATASERVER: Client request from " +  clientsocket.getRemoteSocketAddress().toString() + " is " + jsonreqstr);

                if ( filelocation != null )
                {
                	File fpath = new File(filelocation);
                	if ( fpath.exists() )
                	{
	                	DataOutputStream out = new DataOutputStream(clientsocket.getOutputStream());
	    				InputStream fis = new FileInputStream(fpath);
	    				byte[] bytes = IOUtils.toByteArray(fis);

	    				out.writeInt(bytes.length); // write length of the message
	    				out.write(bytes);           // write the message
	    				
	    				fis.close();
	                	out.close();
                	}
                }  
                in.close();
            }
            catch (Exception e)
            {
            	e.printStackTrace();
            }
            finally {
                try {
					clientsocket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }

		}

	}
	
	public static void main(String[] args) throws IOException 
	{
		parseCmdArguments(args);
		ServerSocket listener = null;
		try {
            new Thread(new CustomHadoopDataServer().new MetaRegistry()).run();

			listener = new ServerSocket(Integer.valueOf(clientPort));
			System.out.println("DATASERVER: Listening for Client request on port " + clientPort);
			while (true) 
			{
                Socket clientsocket = listener.accept();
                new Thread(new CustomHadoopDataServer().new WorkerRunnable(clientsocket)).start();

            }
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			listener.close();
		}
	}

	public class MetaRegistry implements Runnable{
	    public MetaRegistry() {}
		public void run() {
            try {
          	  	
            	String metaServer = PropertyHandler.getInstance().getValue("metaserver_host");
            	int regport = Integer.valueOf(PropertyHandler.getInstance().getValue("metaserver_registryPort"));
            	Socket regsocket = new Socket(metaServer, regport);
            	JSONObject jsonreq = new JSONObject();
            	JSONArray filemetaarr = new JSONArray();
            	jsonreq.put("filemeta", filemetaarr);
          	  	File folder = new File(basePath);
          	  
          	  	File[] listOfFiles = folder.listFiles();
          	  	for (int i = 0; i < listOfFiles.length; i++) 
          	  	{
          	  		if (listOfFiles[i].isFile()) 
          	  		{
	          	        String fname = listOfFiles[i].getName();
	          	        long    flen = listOfFiles[i].length(); // ramanujam
	          	        String filepath = listOfFiles[i].getAbsolutePath();
	          	        InetAddress localip = InetAddress.getLocalHost();
	        	        String localhostaddr = localip.getHostAddress();
	        	        JSONObject jobj = new JSONObject();
	        	        jobj.put("filename", fname);
	              	  
	        	        jobj.put("fileserverpath", localhostaddr+"|"+filepath+"|"+flen ); // ramanujam
	        	        filemetaarr.put(jobj);
          	  		}
          	  	}
          	  jsonreq.put("dataserverport", clientPort);
          	  System.out.println("MetaInfo upload request for files in path " + basePath + " is " + jsonreq.toString());
          	  	
          	  DataOutputStream out = new DataOutputStream(regsocket.getOutputStream());
          	  out.writeUTF(jsonreq.toString());
          	  DataInputStream in = new DataInputStream(regsocket.getInputStream());
              String jsonstr = in.readUTF();
              JSONObject jsonobj = new JSONObject(jsonstr);
              if ( ! jsonobj.get("status").equals("success") )
             	  System.out.println("ERROR: Meta info is not updated in MetaServer");
             
          	  in.close();
          	  out.close();
            }
            catch (Exception e)
            {
            	e.printStackTrace();
            }
		}
	}
	
	public static Options constructPosixOptions()
	   {
	      final Options opt = new Options();
	      opt.addOption("p", true, "(Optional) default is 8084. Port which dataserver listens to service data file read requests");
	      opt.addOption("b", true, "Input base file path directory whose meta info is published to meta server");
	      return opt;
	   }

	public static void parseCmdArguments(String args[])
	{
		try 
		{
			CommandLineParser parser = new BasicParser();
			Options options = constructPosixOptions();
			CommandLine cmd = parser.parse( options, args);
		    if (cmd.hasOption("p")) 
		    {
		    	clientPort = cmd.getOptionValue("p");
		    }
		    else
		    {
		    	clientPort = PropertyHandler.getInstance().getValue("dataserver_defaultclientPort");
		    }
		    if (cmd.hasOption("b")) 
		    {
		    	basePath = cmd.getOptionValue("b");
		    }
		    else
		    {
		    	HelpFormatter formatter = new HelpFormatter();
		    	formatter.printHelp("java -cp <classpath> com.hadoopsimulator.CustomHadoopDataServer ", "", options, "", true);
		    	System.exit(1);
		    }
		  
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}