package com.hadoopsimulator.test;

import com.hadoopsimulator.meta.CustomFileMetaInfo;
import com.hadoopsimulator.utility.CustomHadoopFileUtility;

public class CustomHadoopTest {

	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		//System.out.println(PropertyHandler.getInstance().getValue("metaserver_host"));
		CustomFileMetaInfo meta = CustomHadoopFileUtility.getFileMetaInfo(args[0]);
		if ( meta != null )
		{
		 String msg = new String(CustomHadoopFileUtility.read(meta, 0, 0));
		 System.out.println("Response Message is " + msg);
		}
		else
		{
			 System.out.println("Meta info cannot be retrieved for file " + args[0]);
		}
	}

}
