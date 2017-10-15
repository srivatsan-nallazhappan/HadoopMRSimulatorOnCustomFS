package com.hadoopsimulator.driver;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoopsimulator.input.CustomHadoopInputFormat;
import com.hadoopsimulator.input.CustomHadoopInputSplit;

public class MainMRClientJob extends Configured implements Tool
{

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		// When implementing tool
		Configuration conf = this.getConf();

				// Create job
		Job job = new Job(conf, "Tool Job");
		job.setJarByClass(MainMRClientJob.class);

		job.setMapperClass(CustomHadoopMap.class);
		job.setReducerClass(CustomHadoopReduce.class);
		
		job.setInputFormatClass(CustomHadoopInputFormat.class);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Input
		CustomHadoopInputFormat.setInput(job, "Input1.txt,Input2.txt");

		// Output
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
			
	}

	public static class CustomHadoopMap extends Mapper<NullWritable, Text, Text, Text>
	{
		 public void map(NullWritable key, Text value, Context context) throws 
		 IOException, InterruptedException
		 {
			 
			 CustomHadoopInputSplit split = (CustomHadoopInputSplit ) context.getInputSplit();
             String [] locations =  split.getLocations();       // actual location ip
             String filename     =  split.getDataset();
             InetAddress localip =  InetAddress.getLocalHost(); // local host ip.
             String localhostaddr = localip.getHostAddress();
             
             // write filename, actual location ip, processed location ip
             context.write(new Text(filename),
            	 new Text(locations[0] + "," + localhostaddr ));
             
             
             /*InetAddress addr = InetAddress.getByName("10.194.36.37");
               String host = addr.getHostName();
               System.out.println(host);
              */
	       }
	
	}
	
	public static class CustomHadoopReduce extends Reducer<Text, Text, Text, Text> 
	{
		private int onlyOnce = 0 ;
		public void reduce( Text key, Iterable<Text> values, Context context ) throws 
		IOException, InterruptedException
		{
			for ( Text value: values )
			{
				if ( onlyOnce == 1 )
				{
					System.out.println("Error: map should be called only once");
					break;
				}
				context.write(key, value);
				onlyOnce = 1;
			}
			  
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MainMRClientJob(), args);
		System.exit(res);
	}

}
