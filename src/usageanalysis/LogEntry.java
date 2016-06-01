package usageanalysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

public class LogEntry implements Comparable<LogEntry> {
	private String dateTime;
	private String ipAddress;
	private String level;
	private String location;
	private String errorMsg;
	private String module;
	private String userId;
	public static HashSet<String> totalUsers = new HashSet<>();

	public LogEntry(String dateTime, String ipAddress, String level,
			String location, String errorMsg, String module, String userId) {
		super();
		this.dateTime = dateTime;
		this.ipAddress = ipAddress;
		this.level = level;
		this.location = location;
		this.errorMsg = errorMsg;
		this.module = module;
		this.userId = userId;
	}

	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public int compareTo(LogEntry arg0) {
		try {
			Date currentObjTime = format.parse(dateTime);
			Date otherObjTime = format.parse(arg0.dateTime);
			if (currentObjTime.getTime() > otherObjTime.getTime()) {
				return 1;
			} else
				return -1;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// TODO Auto-generated method stub
		return 0;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}

	public String getModule() {
		return module;
	}

	public void setModule(String module) {
		this.module = module;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public SimpleDateFormat getFormat() {
		return format;
	}

	public void setFormat(SimpleDateFormat format) {
		this.format = format;
	}
}
