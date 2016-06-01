package usageanalysis;

import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import realtime.*;

public class UsageMain implements Runnable {

	public static String inputFilePath ;
	public static String inputFileName;
	LogMonitor logmon = new LogMonitor();

	public static void initJob(Job job) {
		org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setMapperClass(UsageMapper.class);
		job.getConfiguration().set("mapred.mapper.new-api", "true");
		job.getConfiguration().set("mapred.map.tasks", "5");
		job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
		job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
		job.setReducerClass(UsageReducer.class);
		job.getConfiguration().set("mapred.reducer.new-api", "true");
		job.getConfiguration().set("mapred.reduce.tasks", "4");
		job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setOutputValueClass(org.apache.hadoop.io.Text.class);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
	}
	
	@Override
	public void run() {
		try{
			for(File file : logmon.listofFiles){
			try{
					inputFilePath = file.getAbsolutePath();
					inputFileName = file.getName();						
				}catch(SecurityException se){
					se.printStackTrace();
				}
				System.out.println("============== Starting to process(Usage Analysis) : "+inputFileName+" ==============");
				Job job = new Job();
				initJob(job);
				FileInputFormat.setInputPaths(job, inputFilePath);
				FileOutputFormat.setOutputPath(job,
						new Path(logmon.outputFileName + System.currentTimeMillis() + "_usage"));
				job.submit();
				long start = new Date().getTime();
				job.waitForCompletion(true);
				long end = new Date().getTime();
				System.out.println("Finished Usage Analysis on "+inputFileName +" : Time Elapsed "+(end - start));
				Thread.sleep(12000);
			}
			
		}catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
