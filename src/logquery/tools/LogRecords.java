package logquery.tools;

import java.util.HashMap;

public class LogRecords {
	
	private String response;	// 服务器的响应时间，如0.000
	private String cookie;		// 浏览器cookie，如ezq05FI1HecstEorPZPJAg==
	private String timestamp;	// 该次请求的时间戳，如25/Nov/2013:18:26:33
	private String request;		// 在网站上通过何种方式获取了哪些信息，用来记录请求的url与http协议
	private String hostname;	// 访问者的主机名，如xk.2QxD.gr5.XxO
	private String httpstate;	// 状态码，服务器的响应状态(1xx：继续消息；2xx：请求成功；3xx：请求重定向；4xx：客户端错误；5xx：服务器错误)
	private String referer;		// 来源页面，用来记录从那个页面链接访问过来的，可能为“-”
	private String agent;		// 记录客户浏览器的相关信息，可能为“-”
	private String size;		// 该次请求中一共传输的字节数，如1690或者0
	private String ip;			// 访问者的ip地址，如116.112.113.50
	
	
	public LogRecords(){}
	
	/**
	 * 根据一个字符串构造一个LogRecord类，
	 * @param record_str
	 */
	public LogRecords(String record_str){
		
		HashMap<String, String> record_map = new HashMap<String, String>();
		record_map = this.formatLogRecord(record_str);							// 格式化该字符串，并返回一个map
		/*
		 * 逐个从map中取出经过格式化的键值对，并用于构造该LogRecord类
		 */
		if(record_map.containsKey("response")){
			this.response = record_map.get("response");
		}else{
			this.response = null;
			System.err.println("Key is not found: response" );
		}
		
		if(record_map.containsKey("cookie")){
			this.cookie = record_map.get("cookie");
		}else{
			this.cookie = null;
			System.err.println("Key is not found: cookie" );
		}
		
		if(record_map.containsKey("timestamp")){
			this.timestamp = record_map.get("timestamp");
		}else{
			this.timestamp = null;
			System.err.println("Key is not found: timestamp" );
		}

		if(record_map.containsKey("request")){
			this.request = record_map.get("request");
		}else{
			this.request = null;
			System.err.println("Key is not found: request" );
		}
		
		if(record_map.containsKey("hostname")){
			this.hostname = record_map.get("hostname");
		}else{
			this.hostname = null;
			System.err.println("Key is not found: hostname" );
		}
		
		if(record_map.containsKey("httpstate")){
			this.httpstate = record_map.get("httpstate");
		}else{
			this.httpstate = null;
			System.err.println("Key is not found: httpstate" );
		}
		
		if(record_map.containsKey("referer")){
			this.referer = record_map.get("referer");
		}else{
			this.referer = null;
			System.err.println("Key is not found: referer" );	
		}
		
		if(record_map.containsKey("agent")){
			this.agent = record_map.get("agent");
		}else{
			this.agent = null;
			System.err.println("Key is not found: agent" );		
		}
		
		if(record_map.containsKey("size")){
			this.size = record_map.get("size");
		}else{
			this.size = null;
			System.err.println("Key is not found: size" );
		}
		
		if(record_map.containsKey("ip")){
			this.ip = record_map.get("ip");
		}else{
			this.ip = null;
			System.err.println("Key is not found: ip" );		
		}
	}
	
	
	
	/**
	 * 字符串格式化:将读取的每一行日志记录（字符串）做格式化处理,该字符串是从日志源文件access_log中读取的行，形如
	 * 
	 * {"response":"0.033","cookie":"-","timestamp":"25/Nov/2013:18:26:33",
	 * "request":"GET /2QxD/kv9vuX/aararrgeSaBg5largBSla5alr/ HTTP/1.1",
	 * "hostName":"JoDKggB.2QxD.gr5.XxO","httpstate":"200","referer":"-",
	 * "agent":"-","size":"57094","ip":"110.241.20.188"}
	 * 
	 * @param record_str:
	 * @return 经过格式化处理的键值对放入一个map中
	 */
	public HashMap<String, String> formatLogRecord(String record_str){
		
		HashMap<String, String> record_map = new HashMap<String, String>();		// 用于存储格式化后的属性键值对
		
		/*
		 * 去掉行末尾的两个空白符（Tab符）,去掉日志记录字符串首尾的花括号："{"和"}"
		 * 以逗号","切分字符串
		 */
		record_str = record_str.trim().toLowerCase();
		record_str = record_str.substring(1, (record_str.length()-1)); 
		String [] key_value = record_str.split("(?<=\")(\\s*),(\\s*)(?=\")");
		
		/*
		 * 以冒号":"分割每个属性的键值对，
		 * 分别去除key_str和value_str中的首尾空白符和首尾双引号
		 */
		for(String key_value_str: key_value){
			
			String[] str_tmp = key_value_str.split("(?<=\")(\\s*):(\\s*)(?=\")"); 
			String key_str = str_tmp[0].trim();									
			String value_str = str_tmp[1].trim();
			key_str = key_str.substring(1, key_str.length()-1);
			value_str = value_str.substring(1, value_str.length()-1);
			
			// 把经过处理的键值对放入map中
			if(!record_map.containsKey(key_str)){
				record_map.put(key_str, value_str);
			}else{
				System.out.println("This key-value pair is already put into the map.");
			}
		}
		return record_map;
	}
	
	
	
	/**
	 * 根据参数分别截取不同精度的时间戳字符串，
	 * @param precision ，表示精度，参数为0、1、2、3、4、5时精度依次为到年、月、日和小时、分钟、秒
	 * @return 返回一定精度的时间戳字符串
	 */
	public String timePrecision(int precision){
		
		TimeStamps ts = new TimeStamps(this.timestamp);		//格式化时间戳字符串，构建一个TimeStamps类的对象
		String time = "";
		
		switch(precision){
		case 0:
			time = ts.getYear() + "";
			break;
		case  1:
			time = ts.getYear() + "-" + ts.getMonth();
			break;
		case 2:
			time = ts.getYear() + "-" + ts.getMonth() + "-" + ts.getDay();
			break;
		case 3:
			time = ts.getYear() + "-" + ts.getMonth() + "-" + ts.getDay() + ":" + ts.getHour();
			break;
		case 4:
			time = ts.getYear() + "-" + ts.getMonth() + "-" + ts.getDay() + ":" + ts.getHour()
					+ ":" + ts.getMinute();
			break;
		case 5:
			time = ts.getYear() + "-" + ts.getMonth() + "-" + ts.getDay() + ":" + ts.getHour()
					+ ":" + ts.getMinute() + ":" + ts.getSecond();
			break;
		default:
			System.err.println("The input precision is invalid. Please specify a number in {0,1,2,3,4,5}.");
			break;
		}
		
		return time;
	}
		
	/*
	 * The getter and setter of all Class Members
	 */
	public void setResponse(String response) {
		this.response = response;
	}
	public String getResponse() {
		return response;
	}
	public void setCookie(String cookie) {
		this.cookie = cookie;
	}
	public String getCookie() {
		return cookie;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getRequest() {
		return request;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHttpstate(String httpstate) {
		this.httpstate = httpstate;
	}
	public String getHttpstate() {
		return httpstate;
	}
	public void setReferer(String referer) {
		this.referer = referer;
	}
	public String getReferer() {
		return referer;
	}
	public void setAgent(String agent) {
		this.agent = agent;
	}
	public String getAgent() {
		return agent;
	}
	public void setSize(String size) {
		this.size = size;
	}
	public String getSize() {
		return size;
	}
	public void setIP(String ip) {
		this.ip = ip;
	}
	public String getIP() {
		return ip;
	}
	
}
