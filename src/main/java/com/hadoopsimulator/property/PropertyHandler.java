package com.hadoopsimulator.property;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyHandler{

	   private static PropertyHandler instance = null;

	   private Properties props = null;

	   private PropertyHandler(){
	         // Here you could read the file into props object
	         this.props = new Properties();
	         try {
				//props.load(new FileInputStream("src/main/resources/hadoop.properties"));
	            props.load(this.getClass().getClassLoader().getResourceAsStream("hadoop.properties"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   }

	   public static synchronized PropertyHandler getInstance(){
	       if (instance == null)
	           instance = new PropertyHandler();
	       return instance;
	   }

	   public String getValue(String propKey){
	       return this.props.getProperty(propKey);
	   }
	}