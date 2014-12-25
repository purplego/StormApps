package logquery.improvedstorm;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import util.Constants;
import util.FileUtil;
import util.MapTools;
import util.Printer;

import logquery.tools.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PrinterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6552135498175768544L;
	
	private TopologyContext _context;
	
	private long clock4Print; 		// 初始值为该task首次执行的时间，每次向控制台打印信息后会重置为当前时间
	private long clock4Archive; 	// 初始值为该task首次执行的时间，每次归档数据后会重置为当前时间
	
	private HashMap<String, Long> PV_map; 
	private HashMap<String, HashMap<String, Long>> UV_map;
	private HashMap<String, HashMap<String, Long>> status_map;
	private HashMap<String, HashMap<String, Long>> spider_map;

	public void execute(Tuple input) {
		/*
		 * **********************************************************************************************************************
		 * 根据接收到的不同数据流判断相应的处理逻辑，
		 * 其中数据流类型包括， pv-stream、uv-stream、status-stream、spider-stream
		 */
		String source = input.getSourceStreamId();
		if(source == null){
			System.err.println("Merger bolt task - " + _context.getThisTaskIndex()
					+ " : Received a null stream source.");
		}else{//if(source != null)
			/*
			 * 针对PV-stream的判断逻辑
			 * 根据接收到的pv-stream（pvStr）解析出一个pvMap_tmp，
			 * 然后将pvMap_tmp加到PV_map中去
			 */
			if(source.contains("pv-stream")){
				String pvStr = input.getStringByField("pv");
				if(pvStr == null){
					System.err.println("Merger bolt task - " + _context.getThisTaskIndex() 
							+ " : You got a null message of pv-stream from the logAnalyzer bolt.");
				}else{
					
					HashMap<String, Long> pvMap_tmp = new HashMap<String, Long>();
					pvMap_tmp = MapTools.strToMap(pvStr);
					
					PV_map = MapTools.addMap(PV_map, pvMap_tmp);
				}
			}
			
			/*
			 * 针对UV-stream的判断逻辑
			 * 根据接收到的uv-stream（uvStr）解析出一个uvMap_tmp，
			 * 然后将uvMap_tmp加到UV_map中去
			 */
			if(source.contains("uv-stream")){
				String uvStr = input.getStringByField("uv");
				if(uvStr == null){
					System.err.println("Merger bolt task - " + _context.getThisTaskIndex() 
							+ " : You got a null message of uv-stream from the logAnalyzer bolt.");
				}else{
					
					HashMap<String, HashMap<String, Long>> uvMap_tmp = new HashMap<String, HashMap<String, Long>>();
					uvMap_tmp = MapTools.strToMap2(uvStr);
					
					UV_map = MapTools.addMap2(UV_map, uvMap_tmp);
				}
			}
			
			/*
			 * 针对status-stream的判断逻辑
			 * 根据接收到的status-stream（statusStr）解析出一个statusMap_tmp，
			 * 然后将statusMap_tmp加到status_map中去
			 */
			if(source.contains("status-stream")){
				String statusStr = input.getStringByField("status");
				if(statusStr == null){
					System.err.println("Merger bolt task - " + _context.getThisTaskIndex() 
							+ " : You got a null message of status-stream from the logAnalyzer bolt.");
				}else{
					
					HashMap<String, HashMap<String, Long>> statusMap_tmp = new HashMap<String, HashMap<String, Long>>();
					statusMap_tmp = MapTools.strToMap2(statusStr);
					
					status_map = MapTools.addMap2(status_map, statusMap_tmp);
				}
			}
			
			/*
			 * 针对spider-stream的判断逻辑
			 * 根据接收到的spider-stream（spiderStr）解析出一个spiderMap_tmp，
			 * 然后将spiderMap_tmp加到spider_map中去
			 */
			if(source.contains("spider-stream")){
				String spiderStr = input.getStringByField("spider");
				if(spiderStr == null){
					System.err.println("Merger bolt task - " + _context.getThisTaskIndex() 
							+ " : You got a null message of spider-stream from the logAnalyzer bolt.");
				}else{
					
					HashMap<String, HashMap<String, Long>> spiderMap_tmp = new HashMap<String, HashMap<String, Long>>();
					spiderMap_tmp = MapTools.strToMap2(spiderStr);
					
					spider_map = MapTools.addMap2(spider_map, spiderMap_tmp);
				}
			}
		}
		
		/**
		 * 每隔一段时间打印一次统计结果，默认为5分钟；
		 * 每隔一段时间将全局统计结果归档一次，默认为15分钟
		 * 归档后，会将本地的4个统计量清空：PV_map、UV_map、status_map、spider_map
		 */
		if((System.currentTimeMillis() - this.clock4Print) >= ArgsLogquery.printPeriod){
			System.out.println("===================== PV@" + new Date() + " =====================");
			Printer.printMap(PV_map);
			System.out.println();
			
			System.out.println("===================== UV Distribution@" + new Date() + "=====================");
			Printer.printMap2(UV_map);
			System.out.println();
			
			System.out.println("===================== Status Distribution@" + new Date() + "=====================");
			Printer.printMap2(status_map);
			System.out.println();
			
			System.out.println("===================== SpiderSite Distribution@" + new Date() + "=====================");
			Printer.printMap2(spider_map);
			System.out.println();
			
			this.clock4Print = System.currentTimeMillis(); // 重置定时器
		}
		
		if((System.currentTimeMillis() - this.clock4Archive) >= ArgsLogquery.archivePeriod){
			
			FileUtil.appendMapToFile(PV_map, Constants.RESOURCE_DIRECTORY, ArgsLogquery.PV_LOG);
			FileUtil.appendMapToFile2(UV_map, Constants.RESOURCE_DIRECTORY, ArgsLogquery.UV_LOG);
			FileUtil.appendMapToFile2(status_map, Constants.RESOURCE_DIRECTORY, ArgsLogquery.STATUS_LOG);
			FileUtil.appendMapToFile2(spider_map, Constants.RESOURCE_DIRECTORY, ArgsLogquery.SPIDER_LOG);
			
			PV_map.clear();
			UV_map.clear();
			status_map.clear();
			spider_map.clear();
			
			this.clock4Archive = System.currentTimeMillis();
		}
		
		
		
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_context = context;
		
		this.PV_map = new HashMap<String, Long>();
		this.UV_map = new HashMap<String, HashMap<String, Long>>();
		this.status_map = new HashMap<String, HashMap<String, Long>>();
		this.spider_map = new HashMap<String, HashMap<String, Long>>();
		
		clock4Print = System.currentTimeMillis();	// 初始化定时器
		clock4Archive = System.currentTimeMillis();	// 初始化定时器
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
