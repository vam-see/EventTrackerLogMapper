package realtime;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import usageanalysis.*;

public class LogMonitor implements Runnable {
	static Job job;
	public static String outputFileName ;
	public static String inputFilePath ;
	public static String inputFileName;
	public static File[] listofFiles;
	
	public static void initJob(Job job) {
		org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setMapperClass(LogMapper.class);
		job.getConfiguration().set("mapred.mapper.new-api", "true");
		job.getConfiguration().set("mapred.map.tasks", "5");
		job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
		job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
		job.setReducerClass(LogReducer.class);
		job.getConfiguration().set("mapred.reducer.new-api", "true");
		job.getConfiguration().set("mapred.reduce.tasks", "4");
		job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setOutputValueClass(org.apache.hadoop.io.Text.class);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
	}

	public static void main(String args[]) throws Exception {
		
		
		File folder = new File(args[0]);
		listofFiles = folder.listFiles();
		outputFileName = args[1];			
			
		Thread t1 = new Thread(new LogMonitor());
		t1.start();
		Thread t2 = new Thread(new UsageMain());
		t2.start();
		t1.join();
		t2.join();
	}

	@Override
	public void run() {
		while(true){
		// TODO Auto-generated method stub
		try {
			for(File file : listofFiles){
				try{
					inputFilePath = file.getAbsolutePath();
					inputFileName = file.getName();						
				}catch(SecurityException se){
					se.printStackTrace();
				}
				System.out.println("============== Starting to process(Alert Analysis) : "+inputFileName+" ==============");
				job = new Job();			
				initJob(job);
				FileInputFormat.setInputPaths(job, inputFilePath);
				FileOutputFormat.setOutputPath(job,
						new Path(outputFileName + System.currentTimeMillis() + "_realtime"));

				job.submit();
				long startTime = System.currentTimeMillis();
				job.waitForCompletion(true);
				long endTime = System.currentTimeMillis();
				System.out.println("Finished Alert Analysis on "+inputFileName +" : Time Elapsed "+(endTime - startTime));
				

			}
			Thread.sleep(120000);
			
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}

	}

}
