package com.hadoopsimulator.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.hadoopsimulator.meta.CustomFileMetaInfo;
import com.hadoopsimulator.property.PropertyHandler;

public class CustomHadoopFileUtility 
{
	public static CustomFileMetaInfo getFileMetaInfo(String filename) 
	{
		Socket clientsocket = null;
		CustomFileMetaInfo meta = new CustomFileMetaInfo();
        try 
        {
        	  String metaServer = com.hadoopsimulator.property.PropertyHandler.getInstance().getValue("metaserver_host");
  			  int port = Integer.valueOf(PropertyHandler.getInstance().getValue("metaserver_defaultclientPort"));
        	  clientsocket = new Socket(metaServer, port);
        	  JSONObject json = new JSONObject();
        	  json.put("filename", filename);
        	  DataOutputStream out = new DataOutputStream(clientsocket.getOutputStream());
        	  out.writeUTF(json.toString());
        	  DataInputStream in = new DataInputStream(clientsocket.getInputStream());
              String jsonstr = in.readUTF();
              JSONObject jsonobj = new JSONObject(jsonstr);
              meta.setFilename(filename);
              System.out.println("Meta info request is " + json.toString() + " , response is " + jsonstr);
              
              String filestatus = jsonobj.getString("status");
              if ( filestatus != null && filestatus.equals("FileFound"))
              {
	              JSONArray sloc = jsonobj.getJSONArray("serverLocations");
	              String[] slocarr = new String[sloc.length()];
	              for ( int i=0;i<sloc.length();i++ )
	            	  slocarr[i]=sloc.getString(i);
	              
	              JSONArray ploc = jsonobj.getJSONArray("pathLocations");
	              String[] plocarr = new String[ploc.length()];
	              for ( int i=0;i<ploc.length();i++ )
	            	  plocarr[i]=ploc.getString(i);
	            
	              String flen = jsonobj.getString("fileLen");
	              meta.setDataServerPort(jsonobj.getString("dataServerPort"));
	              meta.setLength(flen);
	              meta.setServerLocations(slocarr);
	              meta.setInputPaths(plocarr);
              }
              else
              {
            	  meta = null;
              }
              clientsocket.close();
          }
          catch (Exception e)
          {
               	e.printStackTrace();
               	try {
					clientsocket.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

          }
          return meta;
	}
	
	public static byte[] read(CustomFileMetaInfo meta, int startoffset, int endoffset) 
	{
		byte[] bytes = null;
		InetAddress localip;
		try 
		{
			localip = InetAddress.getLocalHost();
	        String localhostaddr = localip.getHostAddress();
	        String[] locations = meta.getServerLocations();
	        String[] paths = meta.getInputPaths();
			HashSet<String> fileHostSet = new HashSet<String> ();
			for ( String l : locations)
			{
				InetAddress fileHostip = InetAddress.getByName(l);
				String fileHostaddr = fileHostip.getHostAddress();
				fileHostSet.add(fileHostaddr);
			}
			if ( fileHostSet.contains(localhostaddr)) 
			{
				System.out.println(" Short circuit read is performed for file " + meta.getFilename() );
				InputStream is = new FileInputStream(new File(paths[0]));
				bytes = IOUtils.toByteArray(is);
				is.close();
			}
			else
			{
				JSONObject jsonReq = meta.getMetaJSONString();
				jsonReq.put("startoffset", startoffset);
				jsonReq.put("endoffset", endoffset);
				
				int dataserverport = Integer.valueOf(meta.getDataServerPort());
				Socket clientsocket = new Socket(locations[0], dataserverport);
          	  	DataOutputStream out = new DataOutputStream(clientsocket.getOutputStream());
	        	out.writeUTF(jsonReq.toString());
	        	
	        	DataInputStream in = new DataInputStream(clientsocket.getInputStream());
	        	int length = in.readInt();
	        	if(length>0) {
	        		bytes = new byte[length];
	        	    in.readFully(bytes, 0, bytes.length); // read the message
	        	}
				System.out.println(" Read for " + meta.getFilename() + " is performed via dataserver " + locations[0] + ":" + dataserverport );

	        	in.close();
	        	out.close();
	        	clientsocket.close();
			}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return bytes;
	}

}
