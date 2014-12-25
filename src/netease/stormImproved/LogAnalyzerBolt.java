package netease.stormImproved;

import java.util.HashMap;
import java.util.Map;

import netease.tools.ArgsNetease;
import netease.tools.ArgsTools;
import netease.tools.SpiderSite;

import org.json.simple.*;

import util.Constants;
import util.MapTools;

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
	
	private SpiderSite 	spiderSite;				// SpiderSite中初始化存储了10中常见的搜索引擎蜘蛛
	
	private long 		countPV;				// 统计访问量，即页面刷新的次数，即有多少条访问记录
	private HashMap<String, Long> spider_map;	// 统计不同Spider的分布 
	private HashMap<String, Long> status_map;	// 统计不同状态码的分布 

	private long 		timeClock; 				// 初始值为该task首次执行的时间，其后每次向MergerBolt发送数据后会重置为当前时间

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
		String record_str = input.getStringByField("logRecord");// 接收来自spout的数据流数据，FieldName为logRecord
		if(record_str == null){
			System.out.println("logAnalyzer bolt - " + _context.getThisTaskIndex() 
					+ " : Got a null message from the spout.");
			
		}else{
			
			/*
			 * 将接收到的字符串解析为JSON对象并分析处理
			 */
			Object obj = JSONValue.parse(record_str);
			JSONObject record = (JSONObject)obj;
			
			// 统计每个时间段内的PV量
			countPV ++;
			
			// 统计每个时间段内的spider量，并统计不同Spider出现的次数
			String agent = (String)record.get("agent");
			String spiderName = spiderSite.containsSpider(agent);
			if(spiderName != null){

				if(spider_map.containsKey(spiderName)){
					long value = spider_map.get(spiderName);
					value ++;
					spider_map.put(spiderName, value);
				}else{
					spider_map.put(spiderName, 1L);
				}
			}
			
			// 首先规范会每个状态码为1xx，2xx，3xx，4xx，5xx，然后统计每个时间段内不同状态码的分布
			String httpstate = (String)record.get("httpstate");
			if(httpstate.startsWith("1")){
				httpstate = "1xx";
			}else if(httpstate.startsWith("2")){
				httpstate = "2xx";
			}else if(httpstate.startsWith("3")){
				httpstate = "3xx";
			}else if(httpstate.startsWith("4")){
				httpstate = "4xx";
			}else if(httpstate.startsWith("5")){
				httpstate = "5xx";
			}
			
			if(status_map.containsKey(httpstate)){
				long value = status_map.get(httpstate);
				value ++;
				status_map.put(httpstate, value);
			}else{
				status_map.put(httpstate, 1L);
			}
		}
		
		/*
		 * 默认每Constants.archivePeriod间隔,LogAnalyzerBolt就向MergerBolt汇总一次中间统计结果
		 * 每次发送完数据后，重置timeClock为当前时间，
		 * 并清空统计量
		 */
		if((System.currentTimeMillis() - timeClock) >= ArgsNetease.archivePeriod){
			
			String spiderStr = "";
			String statusStr = "";

			if(!spider_map.isEmpty())
				spiderStr = MapTools.mapToString(spider_map);
			
			if(!status_map.isEmpty())
				statusStr = MapTools.mapToString(status_map);
			
			_collector.emit("stat-stream", new Values(countPV, spiderStr, statusStr));
			
			countPV = 0L;
			spider_map.clear();
			status_map.clear();
			timeClock = System.currentTimeMillis();
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
		
		this.spiderSite = new SpiderSite();

		this.countPV = 0L;
		this.spider_map = new HashMap<String, Long>();
		this.status_map = new HashMap<String, Long>();
		
		this.timeClock = System.currentTimeMillis();
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declareStream("stat-stream", new Fields("pv", "spider", "status"));
	}

}
