package com.hadoopsimulator.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.hadoopsimulator.meta.CustomFileMetaInfo;
import com.hadoopsimulator.utility.CustomHadoopFileUtility;

public class CustomHadoopInputReader extends RecordReader<NullWritable, Text> {

	private CustomHadoopInputSplit split;
	private Configuration conf;
	private Text          data;
	private boolean       alreadyRead;
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();
	}	

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return data;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return alreadyRead == true ? 1.0f : 0.0f;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.split = (CustomHadoopInputSplit) arg0;
		this.conf = arg1.getConfiguration();
		alreadyRead = false;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
	// TODO Auto-generated method stub
		if( alreadyRead == true ) return false;
		/////////////////////////////Debug//////////////////////////////////////
		System.out.println( "Dumping split in tasks...");
		split.dump();
		////////////////////////////////////////////////////////////////////////
		CustomFileMetaInfo meta = new CustomFileMetaInfo();
		meta.setFilename(split.getDataset());
		meta.setInputPaths(split.getFullFilePaths());
		meta.setServerLocations(split.getLocations());
		meta.setLength(String.valueOf(split.getLength()));
		String contents = new String(CustomHadoopFileUtility.read(meta, 0, 0));
		data = new Text(contents);
		alreadyRead = true;
		return true;		
	}
}
