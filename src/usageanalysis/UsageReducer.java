package usageanalysis;
import java.io.IOException;
import java.util.*;
import usageanalysis.LogEntry;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class UsageReducer extends Reducer<Text, Text, Text, Text> {
	public String[] temp;
	Map<String, Integer> modCount = new HashMap<String, Integer>();
	Map<String, Integer> userCount = new HashMap<String, Integer>();
	static int users = 0;
	static int sum = 0;
	Iterator<Text> t;
	
	static int counter = 0;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			
			context.write(new Text("*********************************************"), new Text());
			context.write(new Text("UserId	:	" + key), new Text());
			users++;
			int totalUserOpCount = 0;
			
			t = values.iterator();
			while(t.hasNext()){
				String tmp = (t.next()).toString();
				if(!modCount.containsKey(tmp)){
					modCount.put(tmp, 1);
				}
				else{
					int c = modCount.get(tmp);
					modCount.put(tmp, c+1);
				}
				totalUserOpCount++;
			}	
						
			for (Map.Entry<String, Integer> entry : modCount.entrySet()) {
			    String key1 = entry.getKey().toString();
			    Integer value = entry.getValue();
			    if(!userCount.containsKey(key1)){
					userCount.put(key1, 1);
				}
				else{
					int c = userCount.get(key1);
					userCount.put(key1, c+1);
				}
			    float percentage = (value * 100.0f)/totalUserOpCount;
			    context.write(new Text(key1 + "	:	" + String.format("%.2f", percentage) +"\n"), new Text());
			}
			modCount.clear();
			
			if(users == LogEntry.totalUsers.size()){
				context.write(new Text("----------Start Usage Stats for Modules----------"), new Text());
				for (Map.Entry<String, Integer> entry1 : userCount.entrySet()) {
				    String key1 = entry1.getKey().toString();
				    Integer value = entry1.getValue();
				    context.write(new Text("Module	:	" + key1), new Text());
				    context.write(new Text("Users	:	" + value +"\n"), new Text());
				}				
				context.write(new Text("----------End Usage Stats for Modules----------"), new Text());
				userCount.clear();
			}
			
//			context.write(new Text("----------End Usage Stats for Users----------"), new Text());
	}
}
