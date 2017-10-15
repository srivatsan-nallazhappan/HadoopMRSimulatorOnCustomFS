package com.hadoopsimulator.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONObject;

import com.hadoopsimulator.property.PropertyHandler;


public class CustomHadoopMetaServer {
	
	private static HashMap<String,String> metamap = new HashMap<String,String>();
	private static HashMap<String,String> metaFileNametoPort = new HashMap<String,String>();

	private static String clientPort = "";
	public class WorkerRunnable implements Runnable{
	    protected Socket clientsocket = null;
	    public WorkerRunnable(Socket clientSocket) {
	        this.clientsocket = clientSocket;
	    }
		public void run() {
            try {
          	    DataInputStream in = new DataInputStream(clientsocket.getInputStream());
                String jsonreqstr = in.readUTF();
                JSONObject jsonobj = new JSONObject(jsonreqstr);
                String filename = jsonobj.getString("filename");
            	DataOutputStream out = new DataOutputStream(clientsocket.getOutputStream());
            	JSONObject jobj = new JSONObject();
                if ( filename != null && metamap.get(filename) != null )
                {
                	String[] val = metamap.get(filename).split("\\|");
                	String[] sloc = val[0].split(","); 
                	JSONArray sobj = new JSONArray();
                	for (String s:sloc) sobj.put(s);
                	String[] ploc = val[1].split(",");
                	JSONArray pobj = new JSONArray();
                	for (String p:ploc) pobj.put(p);
                	String flen = val[2];  // ramanujam
                	String dataServerPort = metaFileNametoPort.get(filename);     
                	
                	jobj.put("serverLocations", sobj);
                	jobj.put("pathLocations", pobj);
                	
                	jobj.put("dataServerPort", dataServerPort);
                	jobj.put("fileLen", flen); 
                	
                	jobj.put("status", "FileFound");
                	out.writeUTF(jobj.toString());
                }  
                else
                {
                	jobj.put("status", "FileNotFound");
                	out.writeUTF(jobj.toString());
                }

            	System.out.println("METASERVER: Client Metainfo request from " + clientsocket.getRemoteSocketAddress().toString() + " for file " + filename + " , JSON respose is " + jobj.toString());
            	out.close();
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
		ServerSocket listener = null;
		try {
			parseCmdArguments(args);
			/*
			File f = new File(PropertyHandler.getInstance().getValue("metafile_location"));
			InputStream is = new FileInputStream(f);
			BufferedReader buf = new BufferedReader(new InputStreamReader(is));
			String line = buf.readLine();
			while ( line != null )
			{
				String key = line.split(" ")[0];
				String value = line.split(" ")[1];
				metamap.put(key, value);
				line = buf.readLine();
			}
			*/
			
			registerDataServers();
			
			listener = new ServerSocket(Integer.valueOf(clientPort));
        	System.out.println("METASERVER: Listening for Client Metainfo request on port " + clientPort);
        	
			while (true) 
			{
                Socket clientsocket = listener.accept();
                System.out.println("METASERVER: Connection accepted from " + clientsocket.getRemoteSocketAddress().toString());
                new Thread(
                		new CustomHadoopMetaServer().new WorkerRunnable(
                	        clientsocket)
                	).start();

            }
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			listener.close();
		}
	}
	
	private static void registerDataServers()
	{
        new Thread(new CustomHadoopMetaServer().new MetaRegistry()).start();
        return;
	}

	public class MetaRegistry implements Runnable{
	    public MetaRegistry() {}
		public void run()
		{
          try{
        	  int regport = Integer.valueOf(PropertyHandler.getInstance()
               		.getValue("metaserver_registryPort"));
            @SuppressWarnings("resource")
			ServerSocket reglistener = new ServerSocket(regport);
              System.out.println("MetaServer listening for metaregistry from dataserver on port "
              	    + regport);
    			while (true) 
    			{
                    Socket clientsocket = reglistener.accept();
                    System.out.println("METASERVER: DataServer registration Connection accepted from "
                       + clientsocket.getRemoteSocketAddress().toString());
              	    DataInputStream in = new DataInputStream(clientsocket.getInputStream());
                    String jsonreqstr = in.readUTF();
                    JSONObject jsonobj = new JSONObject(jsonreqstr);
                    String dataserverport = jsonobj.getString("dataserverport");
                    
                    System.out.println("METASERVER: Metainfo upload request from " + clientsocket.getRemoteSocketAddress().toString() + " is " + jsonreqstr );

                    JSONArray filemetaarr = jsonobj.getJSONArray("filemeta");
                    if ( filemetaarr != null && filemetaarr.length() > 0 )
                    {
                    	for ( int i=0; i<filemetaarr.length(); i++ )
                    	{
                    		JSONObject filemeta = filemetaarr.getJSONObject(i);
                    		String key = filemeta.getString("filename");
                    		String value = filemeta.getString("fileserverpath");
            				metamap.put(key, value);
            				metaFileNametoPort.put(key, dataserverport);
                    	}
                    	DataOutputStream out = new DataOutputStream(clientsocket.getOutputStream());
                    	JSONObject jobj = new JSONObject();
                    	jobj.put("status", "success");
                    	out.writeUTF(jobj.toString());
                    	out.close();
                    }  
                    in.close();
                    clientsocket.close();
                }
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
		    	clientPort = PropertyHandler.getInstance().getValue("metaserver_defaultclientPort");
		    }
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
