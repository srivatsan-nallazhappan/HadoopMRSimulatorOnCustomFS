package com.hadoopsimulator.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.hadoopsimulator.meta.CustomFileMetaInfo;
import com.hadoopsimulator.utility.CustomHadoopFileUtility;


public class CustomHadoopInputFormat extends InputFormat<NullWritable, Text >  {

	
	// for now, only a simple method will do, later keep a more robust object inside conf!
	public static void setInput(Job job ,  String commaSeperatedInput)
	{
	  	Configuration conf = job.getConfiguration();
		String files = conf.get("mapreduce.custominput.files");
		conf.set("mapreduce.custominput.files", files == null ? 
				     commaSeperatedInput : files + StringUtils.COMMA_STR + commaSeperatedInput);
	}
	
	// return string array of input file names, comma seperated
	public static String getInput(Job job )
	{
		Configuration conf = job.getConfiguration();
		return conf.get("mapreduce.custominput.files");
	}
	
    
	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new CustomHadoopInputReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException
	{
		
		// TODO Auto-generated method stub
		Configuration conf = arg0.getConfiguration();
		String fileList = conf.get("mapred.custominput.files");
		String[] files =  StringUtils.split(fileList);
		
		List<InputSplit> splits = new ArrayList<InputSplit>();
		
		int loopCnt = 0;
		for (;loopCnt < files.length; loopCnt++)
		{
			String file = files[loopCnt];
			if ( file == null || file.isEmpty())
				continue;
		    // get meta data for every file
			CustomFileMetaInfo m = CustomHadoopFileUtility.getFileMetaInfo(file);
			// -1 indicates full file. for now end-points is same as hosts - to do
			CustomHadoopInputSplit split = new CustomHadoopInputSplit(m.getFilename(), m.getInputPaths(), 
				0, Long.parseLong(m.getLength()), m.getServerLocations(), m.getDataServerPort() );
			splits.add(split);			
		    split.dump(); // debug ...
		}
				
		return splits;
	}
	
	

}
