package logquery.storm;

import java.util.HashMap;
import java.util.Map;

import util.Constants;
import util.MapTools;

import logquery.tools.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogAnalyzerBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 8963886289234350642L;
	
	private TopologyContext _context;
	private OutputCollector _collector;
	
	private long timeClock; // 初始值为该task首次执行的时间，其后每次向MergerBolt发送数据后会重置为当前时间
	
	/*
	 * PV 用于统计点击量
	 * 		HashMap<String, Long>，String为以小时为进度的时间戳，Long为点击量 
	 * UV 用于统计不同的IP地址个数，24小时内同一个IP地址只算一次
	 * 		HashMap<String, HashMap<String, Long>>
	 *		外层Map的Key为以天为精度的时间戳，内层Map的key为IP地址，value为IP地址出现的次数
	 * status 用于统计状态码分布
	 * 		HashMap<String, HashMap<String, Long>>
	 * 		外层Map的Key为以小时为精度的时间戳，内层Map的key为状态码，value为该状态码出现的次数
	 * spider 用于统计浏览器蜘蛛爬虫的分布
	 * 		HashMap<String, HashMap<String, Long>>
	 * 		外层Map的Key为以小时为精度的时间戳，内层Map的key为spider，value为该spider出现的次数
	 * 
	 * 时间戳Timestamp的格式形如：2013-11-35（精度为day）或2013-11-25:23（精度为hour）
	 * IP地址形式为：157.55.34.183
	 * 状态码为：1xx：继续消息；2xx：请求成功；3xx：请求重定向；4xx：客户端错误；5xx：服务器错误
	 * spider包括：googlebot/baiduspider/baidugame/msnbot/bingbot/ahrefsbot/360spider/nutch/sosospider/"sogou web spider
	 */
	private HashMap<String, Long> PV_map; 
	private HashMap<String, HashMap<String, Long>> UV_map;
	private HashMap<String, HashMap<String, Long>> status_map;
	private HashMap<String, HashMap<String, Long>> spider_map;
	
	private SpiderSite spiderSite;								// SpiderSite中初始化存储了10中常见的搜索引擎蜘蛛

	/**
	 * The bolt will receive log records from the spout and process them to Normalize these lines
	 * The normalize will put the the logRecords to lower case
	 * and split the logRecord to get all 10 attributes of a single log record
	 */
	public void execute(Tuple input) {

		/**
		 * 对正常的接收消息（非空且不为{}）进行处理，主要统计4个统计量，分别PV、UV、status和spider agent，
		 * 分别存储在PV_map、UV_map、status_map、spider_map中
		 */
		String record_str = input.getStringByField("logRecord");// 接收来自spout的数据流数据，FiledName为logRecord
		if(record_str == null){
			System.out.println("logAnalyzer bolt - " + _context.getThisTaskIndex() 
					+ " : Got a null message from the spout.");
			
		}else{
			LogRecords record = new LogRecords(record_str);		// 解析接收到的字符串，并根据该字符串创建一个LogRecords的对象：record
			String stamp = record.timePrecision(3);				// 解析LogRecords对象中的时间戳，截取精度默认为小时：stamp
			
			/*
			 * 以小时为精度，统计一天之中 每个小时的PV：Page View; 
			 * 一天24小时之内，每来一条记录便会统计一次
			 */
			if(PV_map.containsKey(stamp)){
				long pv_count = PV_map.get(stamp);
				pv_count ++;
				PV_map.put(stamp, pv_count);
			}else{
				PV_map.put(stamp, 1L);
			}
			
			/*
			 * 以小时为精度，统计一天之中 每个小时的状态码分布：status; 
			 * 一天24小时之内，每来一条请求便会统计一次
			 */
			String httpstate = record.getHttpstate();
			HashMap<String, Long> httpstate_map = new HashMap<String, Long>();
			
			if(status_map.containsKey(stamp)){
				httpstate_map = status_map.get(stamp);
				if(httpstate_map.containsKey(httpstate)){
					long status_count = httpstate_map.get(httpstate);
					status_count ++;
					httpstate_map.put(httpstate, status_count);
					
				}else{
					httpstate_map.put(httpstate, 1L);
				}
				status_map.put(stamp, httpstate_map);
				
			}else{
				httpstate_map.put(httpstate, 1L);
				status_map.put(stamp, httpstate_map);
			}
			
			/*
			 * 以小时为精度，统计一天之中 每个小时的搜索引擎蜘蛛的分布
			 * 
			 * 首先判断这个agent是不是搜索引擎蜘蛛，
			 * 如果spiderName == null，表示不是蜘蛛，DO NOTHING；
			 * 如果spiderName != null，表示该agent是蜘蛛，修改spider_map变量
			 */
			String agent = record.getAgent();
			String spiderName = spiderSite.containsSpider(agent);
			
			if(spiderName == null){
				// Do nothing
			}else{
				agent = spiderName;
				HashMap<String, Long> agent_map = new HashMap<String, Long>();
				
				if(spider_map.containsKey(stamp)){
					agent_map = spider_map.get(stamp);
					if(agent_map.containsKey(agent)){
						long spider_count = agent_map.get(agent);
						spider_count ++;
						agent_map.put(agent, spider_count);
						
					}else{
						agent_map.put(agent, 1L);
					}
					spider_map.put(stamp, agent_map);
					
				}else{
					agent_map.put(agent, 1L);
					spider_map.put(stamp, agent_map);
				}
			}
			
			/*
			 * 以天为精度，统计一天之内的UV：Unique Vistor; 
			 * 一天24小时之内，所有相同的IP只会统计一次
			 */
			stamp = record.timePrecision(2);	// 解析LogRecords对象中的时间戳，截取精度为day：stamp
			String ip = record.getIP();
			HashMap<String, Long> ip_map = new HashMap<String, Long>();
			
			if(UV_map.containsKey(stamp)){
				ip_map = UV_map.get(stamp);
				if(ip_map.containsKey(ip)){
					long uv_count = ip_map.get(ip);
					uv_count ++;
					ip_map.put(ip, uv_count);
				}else{
					ip_map.put(ip, 1L);
				}
				UV_map.put(stamp, ip_map);
				
			}
			else{
				ip_map.put(ip, 1L);
				UV_map.put(stamp, ip_map);
			}
		}
		
		/**
		 * 以一定的时间粒度，把局部统计结果发送给下一级MergerBolt
		 * 默认时间粒度为5分钟，且发送过后，会将本地的4个统计量清空：PV_map、UV_map、status_map、spider_map
		 * 
		 * 以一个starttime作为整个task开始执行的时间，然后每次接收到一条记录（执行一次execute），
		 * 就判断当前时间与starttime之间的时间差是否达到发送周期，如果达到，才发送；发送后，重置starttime
		 * 
		 */
		if((System.currentTimeMillis() - timeClock) >= ArgsLogquery.transPeriod){
			
			if(!PV_map.isEmpty()){
				String pvStr = MapTools.mapToString(PV_map);
				_collector.emit("pv-stream", new Values(pvStr));			// 数据流发送
				PV_map.clear();
			}else{
				System.err.println("logAnalyzer bolt task - " + _context.getThisTaskIndex() 
						+ " : The PV_map is empty now.");
			}
			
			if(!UV_map.isEmpty()){
				String uvStr = MapTools.mapToString2(UV_map);
				_collector.emit("uv-stream", new Values(uvStr));			// 数据流发送
				UV_map.clear();
			}else{
				System.err.println("logAnalyzer bolt task - " + _context.getThisTaskIndex() 
						+ " : The UV_map is empty now.");
			}
			
			if(!status_map.isEmpty()){
				String statusStr = MapTools.mapToString2(status_map);
				_collector.emit("status-stream", new Values(statusStr));	// 数据流发送
				status_map.clear();
			}else{
				System.err.println("logAnalyzer bolt task - " + _context.getThisTaskIndex() 
						+ " : The status_map is empty now.");
			}
			
			if(!spider_map.isEmpty()){
				String spiderStr = MapTools.mapToString2(spider_map);
				_collector.emit("spider-stream", new Values(spiderStr));	// 数据流发送
				spider_map.clear();
			}else{
				System.err.println("logAnalyzer bolt task - " + _context.getThisTaskIndex() 
						+ " : The spider_map is empty now.");
			}
			
			timeClock = System.currentTimeMillis();			// 重置定时发送的起始时间
		}else{
			// Do nothing 
		}
	}


	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		/*
		 * 变量初始化
		 */
		_context = context;
		_collector = collector;
		
		this.PV_map = new HashMap<String, Long>();
		this.UV_map = new HashMap<String, HashMap<String, Long>>();
		this.status_map = new HashMap<String, HashMap<String, Long>>();
		this.spider_map = new HashMap<String, HashMap<String, Long>>();
		
		this.spiderSite = new SpiderSite();
		
		this.timeClock = System.currentTimeMillis();
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declareStream("pv-stream", new Fields("pv"));
		declarer.declareStream("uv-stream", new Fields("uv"));
		declarer.declareStream("status-stream", new Fields("status"));
		declarer.declareStream("spider-stream", new Fields("spider"));
	}

}
