package usageanalysis;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class UsageMapper extends Mapper<LongWritable , Text, Text, Text> {

	protected void map(LongWritable  key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String currentValue = value.toString().trim();
//		System.out.println(currentValue);
		String[] line = currentValue.split("_");
		String mappingValue = new String();
		int lineLength = line.length;
		if(lineLength > 1){
			for(int i = 0; i < lineLength; i++){
				line[i] = line[i].trim();
			}
			String user = line[6];
			LogEntry.totalUsers.add(user);
			mappingValue = mappingValue + line[5];
			context.write(new Text(user), new Text(mappingValue));	
		}
		
	}
}
