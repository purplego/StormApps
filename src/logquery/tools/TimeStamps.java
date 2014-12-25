package logquery.tools;

public class TimeStamps {
	
	private int year;
	private int month;		// 值的范围为1~12
	private int day;		// 值的范围为1~31
	private int hour;		// 值的范围为0~23
	private int minute;		// 值的范围为0~59
	private int second;		// 值的范围为0~59
	
	public TimeStamps(){}
	/**
	 * 根据给定的字符串创建一个TimeStamps对象
	 * formatType格式如， "25/nov/2013:23:00:26"
	 */
	public TimeStamps(String timestamp){
		
		/*
		 * 把给定字符串切分成两部分，分别为日期和时间；
		 * 然后再次切分日期和时间，获得年、月、日，以及小时、分钟和秒
		 */
		int index = timestamp.indexOf(":");
		String date = timestamp.substring(0, index);
		String time = timestamp.substring(index + 1);
		
		String date_array[] = date.split("/");			// "25/nov/2013"
		String time_array[] = time.split(":");			// "23:00:26"
		
		/*
		 * 解析切分所获得的年、月、日和小时，并根据所获得的值构建TimeStamps对象
		 */
		this.year = Integer.parseInt(date_array[2]);	// 解析年
		
		if(date_array[1].contains("jan")){				// 解析月份
			this.month = 1;
		}else if(date_array[1].contains("feb")){
			this.month = 2;
		}else if(date_array[1].contains("mar")){
			this.month = 3;
		}else if(date_array[1].contains("apr")){
			this.month = 4;
		}else if(date_array[1].contains("may")){
			this.month = 5;
		}else if(date_array[1].contains("jun")){
			this.month = 6;
		}else if(date_array[1].contains("jul")){
			this.month = 7;
		}else if(date_array[1].contains("aug")){
			this.month = 8;
		}else if(date_array[1].contains("sep")){
			this.month = 9;
		}else if(date_array[1].contains("oct")){
			this.month = 10;
		}else if(date_array[1].contains("nov")){
			this.month = 11;
		}else if(date_array[1].contains("dec")){
			this.month = 12;
		}else{
			System.err.println("Error month. There is no match to the specified month string.");
		}
		
		this.day = Integer.parseInt(date_array[0]);		// 解析日
		this.hour = Integer.parseInt(time_array[0]);	// 解析小时
		this.minute = Integer.parseInt(time_array[1]);	// 解析分钟
		this.second = Integer.parseInt(time_array[2]);	// 解析秒数
	}
	
	/**
	 * 将一个TimeStamps类的对象转换为一定格式的字符串
	 * 字符串formatType格式如， "2013-11-25:23:00:26"
	 * @param timestamp
	 */
	public String toString(){
		return (this.year + "-" + this.month + "-" + this.day + ":" 
				+ this.hour + ":" + this.minute + ":" + this.second);
	}
	
	/*
	 * The setters and getters of the Class Members
	 */
	public void setYear(int year) {
		this.year = year;
	}
	public int getYear() {
		return year;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public int getMonth() {
		return month;
	}
	public void setDay(int day) {
		this.day = day;
	}
	public int getDay() {
		return day;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public int getHour() {
		return hour;
	}
	public void setMinute(int minute) {
		this.minute = minute;
	}
	public int getMinute() {
		return minute;
	}
	public void setSecond(int second) {
		this.second = second;
	}
	public int getSecond() {
		return second;
	}
}
