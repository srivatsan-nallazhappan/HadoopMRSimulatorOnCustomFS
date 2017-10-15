package com.hadoopsimulator.meta;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

public class CustomFileMetaInfo 
{
	private String filename;
	private String length;
	private String[] serverLocations;
	private String[] inputPaths;
	private String dataServerPort;

	
	public String getDataServerPort() {
		return dataServerPort;
	}
	public void setDataServerPort(String dataServerPort) {
		this.dataServerPort = dataServerPort;
	}
	
	public void setLength(String length)
	{
		this.length = length;
	}
	public String getLength()
	{
		return length;
	}
	public String[] getServerLocations() {
		return serverLocations;
	}
	public void setServerLocations(String[] serverLocations) {
		this.serverLocations = serverLocations;
	}
	public String[] getInputPaths() {
		return inputPaths;
	}
	public void setInputPaths(String[] inputPaths) {
		this.inputPaths = inputPaths;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public JSONObject getMetaJSONString()
	{
		JSONObject jobj = new JSONObject();
    	JSONArray sobj = new JSONArray();
    	JSONArray pobj = new JSONArray();
    	for (String s:serverLocations) sobj.put(s);
    	for (String p:inputPaths) pobj.put(p);
    	jobj.put("serverLocations", sobj);
    	jobj.put("pathLocations", pobj);
    	
    	return jobj;
	}
}
