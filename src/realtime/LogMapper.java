package realtime;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String currentValue = value.toString().trim();
		currentValue = currentValue.replaceAll("( )+", " ");
		String[] line = currentValue.split("_");
		String mappingValue = new String();
		if (line.length > 5) {
			if ((line[2].contains("ERROR")) || (line[2].contains("WARN"))
					|| (line[2].contains("FATAL"))) {
				mappingValue = line[0] + "&" + line[1] + "&" + line[3] + "&"
						+ line[4] + "&" + line[5] + "&" + line[6];
				context.write(new Text(line[2]), new Text(mappingValue));
			}
		}
	}
}
