package netease.stormImproved;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import util.Constants;
import util.FileUtil;
import util.MapTools;
import util.RemoteMySQLAccess;

import netease.tools.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MergerBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6552135498175768544L;
	
	private TopologyContext _context;
	
	/*
	 * 用于统计PV总数和蜘蛛网址总数的变量，以及统计不同Spider的分布和不同状态码的分布
	 */
	private long countPV;
	private long countSpider;
	private HashMap<String, Long> spider_map; 
	private HashMap<String, Long> status_map; 
	
	private long timeClock;		// 用于定时归档结果的计时器数据
	
	public void execute(Tuple input) {

		countPV += input.getLongByField("pv");
		
		String spiderStr = input.getStringByField("spider");
		String statusStr = input.getStringByField("status");
		
		HashMap<String, Long> spiderTmp_map = new HashMap<String, Long>();
		HashMap<String, Long> statusTmp_map = new HashMap<String, Long>();
		
		if(!spiderStr.equals("")){
			spiderTmp_map = MapTools.strToMap(spiderStr);
		}
		if(!statusStr.equals("")){
			statusTmp_map = MapTools.strToMap(statusStr);
		}
		spider_map = MapTools.addMap(spider_map, spiderTmp_map);
		status_map = MapTools.addMap(status_map, statusTmp_map);
		
		/*
		 * 默认每Constants.archivePeriod间隔,MergerBolt就向把统计结果归档（写文件、数据库等）
		 * 每次归档完数据后，重置timeClock为当前时间，并清空统计量
		 */
		if((System.currentTimeMillis() - timeClock) >= ArgsNetease.archivePeriod){
			
			// ******************************************* 数据规范化 ***********************************************
			long googlebotNum = 0L, baiduspiderNum = 0L, baidugameNum = 0L, msnbotNum = 0L, 
					bingbotNum = 0L, ahrefsbotNum = 0L, spider360Num = 0L, nutchNum = 0L, 
					sosospiderNum = 0L, sogou_web_spiderNum = 0L;
			long one = 0L, two = 0L, three = 0L, four = 0L, five = 0L;
			
			if(!spider_map.isEmpty()){
				
				if(spider_map.containsKey("googlebot"))
					googlebotNum = spider_map.get("googlebot");
				if(spider_map.containsKey("baiduspider"))
					baiduspiderNum = spider_map.get("baiduspider");
				if(spider_map.containsKey("baidugame"))
					baidugameNum = spider_map.get("baidugame");
				if(spider_map.containsKey("msnbot"))
					msnbotNum = spider_map.get("msnbot");
				if(spider_map.containsKey("bingbot"))
					bingbotNum = spider_map.get("bingbot");
				if(spider_map.containsKey("ahrefsbot"))
					ahrefsbotNum = spider_map.get("ahrefsbot");
				if(spider_map.containsKey("360spider"))
					spider360Num = spider_map.get("360spider");
				if(spider_map.containsKey("nutch"))
					nutchNum = spider_map.get("nutch");
				if(spider_map.containsKey("sosospider"))
					sosospiderNum = spider_map.get("sosospider");
				if(spider_map.containsKey("sogou web spider"))
					sogou_web_spiderNum = spider_map.get("sogou web spider");
				
				countSpider = googlebotNum + baiduspiderNum + baidugameNum + msnbotNum + bingbotNum
							+ ahrefsbotNum + spider360Num + nutchNum + sosospiderNum + sogou_web_spiderNum;
			}
			
			if(!status_map.isEmpty()){
				
				if(status_map.containsKey("1xx"))
					one = status_map.get("1xx");
				if(status_map.containsKey("2xx"))
					two = status_map.get("2xx");
				if(status_map.containsKey("3xx"))
					three = status_map.get("3xx");
				if(status_map.containsKey("4xx"))
					four = status_map.get("4xx");
				if(status_map.containsKey("5xx"))
					five = status_map.get("5xx");
			}
			
			// TO DO 写文件、写数据库等归档操作
			SimpleDateFormat datef = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
			
			// ******************************************* 写数据库 ***********************************************
			try{
				RemoteMySQLAccess rma = new RemoteMySQLAccess();
				
				rma.execute("insert into pv (count) value ('"+countPV+"')");
				
				rma.execute("insert into spider " +
							"(total, googlebot, baiduspider, baidugame, msnbot, " +
							"bingbot, ahrefsbot, spider360, nutch, sosospider, sogou_web_spider) " +
							"value ('"+countSpider+"', " +
									"'"+googlebotNum+"', '"+baiduspiderNum+"', '"+baidugameNum+"', " +
									"'"+msnbotNum+"', '"+bingbotNum+"', '"+ahrefsbotNum+"', " +
									"'"+spider360Num+"', '"+nutchNum+"', '"+sosospiderNum+"', " +
									"'"+sogou_web_spiderNum+"')");
									
				rma.execute("insert into httpstate " +
								"(one_xx, two_xx, three_xx, four_xx, five_xx) " +
								"value ('"+one+"', '"+two+"', '"+three+"', '"+four+"', '"+five+"')");
									
									
				
			}catch(Exception ex){
				System.err.println("Error : " + ex.toString());
				 FileUtil.appendStrToFile(datef.format(new Date()) + "\tError:\t" + ex.toString(), 
						  Constants.RESOURCE_DIRECTORY, "mySQL.log");
			}
			
			// ******************************************** 写文件 **********************************************

			// 规定时间格式为yyyy-MM-dd:HH:mm:ss
			
			String pvResult = datef.format(new Date()) + "," + countPV;
			
			String spiderResult = datef.format(new Date()) + "," + countSpider  + "," + googlebotNum  + "," 
									+ baiduspiderNum  + "," + baidugameNum  + "," + msnbotNum  + "," 
									+ bingbotNum + "," + ahrefsbotNum  + "," + spider360Num  + "," 
									+ nutchNum  + "," + sosospiderNum  + "," + sogou_web_spiderNum;
			
			String statusResult = datef.format(new Date()) + "," + one  + "," 
									+ two + "," + three + "," + four + "," + five;
			
			FileUtil.appendStrToFile(pvResult, Constants.RESOURCE_DIRECTORY, ArgsNetease.PV_LOG);
			FileUtil.appendStrToFile(spiderResult, Constants.RESOURCE_DIRECTORY, ArgsNetease.SPIDER_LOG);
			FileUtil.appendStrToFile(statusResult, Constants.RESOURCE_DIRECTORY, ArgsNetease.STATUS_LOG);
			
			// **************************************** 清空各种变量 **********************************************
			countPV = 0L;
			countSpider = 0L;
			spider_map.clear();
			status_map.clear();
			timeClock = System.currentTimeMillis();
			
		}
		
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_context = context;
		
		this.countPV = 0L;
		this.countSpider = 0L;
		this.spider_map = new HashMap<String, Long>();
		this.status_map = new HashMap<String, Long>();
		
		this.timeClock = System.currentTimeMillis();
		
		FileUtil.appendStrToFile("time,pvCount", Constants.RESOURCE_DIRECTORY, ArgsNetease.PV_LOG);
		FileUtil.appendStrToFile("time,totalSpider,googlebot,baiduspider,baidugame,msnbot," +
									"bingbot,ahrefsbot,spider360,nutch,sosospider,sogou_web_spider", 
									Constants.RESOURCE_DIRECTORY, ArgsNetease.SPIDER_LOG);
		FileUtil.appendStrToFile("time,1xx,2xx,3xx,4xx,5xx", Constants.RESOURCE_DIRECTORY, ArgsNetease.STATUS_LOG);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
