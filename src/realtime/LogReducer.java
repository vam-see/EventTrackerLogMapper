package realtime;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class LogReducer extends Reducer<Text, Text, Text, Text> {
	int count = 0;
	@Override
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		HashMap<String, String> errorMap = new HashMap<String, String>();
		AlertMail mailObj = AlertMail.getMailObj();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date lastSentTime = null;
		ArrayList<LogEntry> logEntries = new ArrayList<LogEntry>();
			for (Text value : values) {
		
				String alertDetails[] = value.toString().split("&");
				
				String dateTime = alertDetails[0];
				String ipAddress = alertDetails[1];
				String location = alertDetails[2];
				String errorMsg = alertDetails[3];
				String module = alertDetails[4];
				String userId = alertDetails[5];
				LogEntry entry = new LogEntry(dateTime,ipAddress,key.toString(),location,errorMsg,module,userId);
				logEntries.add(entry);
			}
			Collections.sort(logEntries);
			for(LogEntry currentLogEntry: logEntries){
				boolean sendMail = false;
				String mapKey = key.toString() + "#" + currentLogEntry.getLocation() + "#"
						+ currentLogEntry.getErrorMsg() + "#" + currentLogEntry.getModule();
				if (errorMap.containsKey(mapKey)) {
					Date currentDateTime = null;
					long timeInterval = -1;
					try {
						currentDateTime = format.parse(currentLogEntry.getDateTime());
						lastSentTime = format.parse(errorMap.get(mapKey));
						timeInterval = currentDateTime.getTime()
								- lastSentTime.getTime();
						if (timeInterval / (60 * 1000) >= 5) {
							errorMap.put(mapKey, currentLogEntry.getDateTime());
							sendMail = true;
						}
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				else{
					errorMap.put(mapKey, currentLogEntry.getDateTime());
					sendMail = true;
				}
				if (sendMail) {
					String currentTime = format.format(Calendar.getInstance().getTime());
//					System.out.println(key.toString()+" "+currentLogEntry.getLocation()+" "+currentLogEntry.getErrorMsg()+" "+currentLogEntry.getModule()+" "+currentLogEntry.getDateTime()+" "+currentLogEntry.getUserId());
					String mailId = new String();
					if(currentLogEntry.getLevel().contains("WARN")) mailId = "alertt725+1@gmail.com";
					else if(currentLogEntry.getLevel().contains("ERROR")) mailId = "alertt725+2@gmail.com";
					else mailId = "alertt725+3@gmail.com";

					 mailObj.sendMail(mailId,
					 "("+currentLogEntry.getLevel().trim()+")Tax Calculator Module Malfunctioned",
					 "Hello,\nThe Application has detected a problem \nSeverity Level: "+key.toString()+"\nUser : "+currentLogEntry.getUserId()+"\nModule: "+currentLogEntry.getModule()+"\nCode Location: "+currentLogEntry.getLocation()+"\nError Msg:"+currentLogEntry.getErrorMsg()+"\n\nThanks,\nSystem Admin Team");
					context.write(new Text(currentLogEntry.getModule() +" "+ key +" "+currentLogEntry.getDateTime()+" "+currentTime+" -  Mailed " + mailId),new Text());

				}
				
			}
		}
	}

// }

// String classDetails = alertDetails[3];