package netease.storm;

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
	
	/*
	 * 用于定时归档结果的计时器数据
	 */
	private long timeClock4PV;
	private long timeClock4Spider;
	private long timeClock4Status;
	
	public void execute(Tuple input) {
		/*
		 * 规定时间格式为2014-06-20:09:05:50
		 */
		SimpleDateFormat datef = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
		/*
		 * 根据接收到的不同数据流判断相应的处理逻辑，
		 * 其中数据流类型包括， pv-stream、spider-stream
		 */
		String source = input.getSourceStreamId();
		if(source == null){
			System.err.println("Merger bolt task - " + _context.getThisTaskIndex()
					+ " : Received a null stream source.");
		}else{//if(source != null)
			/*
			 * **********************************************************************************************************************
			 * 针对PV-stream的判断逻辑
			 */
			if(source.equals("pv-stream")){
				countPV += input.getLongByField("pv");
				
				/*
				 * 默认每Constants.archivePeriod间隔,MergerBolt就向把统计结果归档（写文件、数据库等）
				 * 每次归档完数据后，重置timeClock为当前时间，并清空统计量
				 */
				if((System.currentTimeMillis() - timeClock4PV) >= ArgsNetease.archivePeriod){
					
					// TO DO 写文件、写数据库等归档操作
					// 写数据库
					try{
						RemoteMySQLAccess rma = new RemoteMySQLAccess();
						rma.execute("insert into pv (count) value ('"+countPV+"')");
						
					}catch(Exception ex){
						System.err.println("Error : " + ex.toString());
						FileUtil.appendStrToFile(datef.format(new Date()) + "\tError:\t" + ex.toString(), 
								  Constants.RESOURCE_DIRECTORY, "mySQL.log");
					}
					
					// 写文件
					String result = datef.format(new Date()) + "," + countPV;
					FileUtil.appendStrToFile(result, Constants.RESOURCE_DIRECTORY, ArgsNetease.PV_LOG);
					
					countPV = 0L;
					timeClock4PV = System.currentTimeMillis();
				}
			}
			
			/*
			 * **********************************************************************************************************************
			 * 针对spider-stream的判断逻辑
			 */
			if(source.equals("spider-stream")){
				
				String spiderStr =  input.getStringByField("spider");
				if(spiderStr == null){
					System.err.println("Merger bolt task - " + _context.getThisTaskIndex() 
							+ " : You got a null message of spider-stream from the logAnalyzer bolt.");
				}else{
					HashMap<String, Long> tmp_map = new HashMap<String, Long>();
					tmp_map = MapTools.strToMap(spiderStr);
					spider_map = MapTools.addMap(spider_map, tmp_map);
				}
				
				if((System.currentTimeMillis() - timeClock4Spider) >= ArgsNetease.archivePeriod){
					
					if(!spider_map.isEmpty()){
						long googlebotNum = 0L, baiduspiderNum = 0L, baidugameNum = 0L;
						long msnbotNum = 0L, bingbotNum = 0L, ahrefsbotNum = 0L, spider360Num = 0L;
						long nutchNum = 0L, sosospiderNum = 0L, sogou_web_spiderNum = 0L;
						
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
						try{
							RemoteMySQLAccess rma = new RemoteMySQLAccess();
							
							rma.execute("insert into spider " +
									"(total, googlebot, baiduspider, baidugame, msnbot, " +
									"bingbot, ahrefsbot, spider360, nutch, sosospider, sogou_web_spider) " +
									"value ('"+countSpider+"', " +
											"'"+googlebotNum+"', '"+baiduspiderNum+"', '"+baidugameNum+"', " +
											"'"+msnbotNum+"', '"+bingbotNum+"', '"+ahrefsbotNum+"', " +
											"'"+spider360Num+"', '"+nutchNum+"', '"+sosospiderNum+"', " +
											"'"+sogou_web_spiderNum+"')");
							
							
						}catch(Exception ex){
							System.err.println("Error : " + ex.toString());
							FileUtil.appendStrToFile(datef.format(new Date()) + "\tError:\t" + ex.toString(), 
									  Constants.RESOURCE_DIRECTORY, "mySQL.log");
						}
						
						// 写文件
						String result = datef.format(new Date()) + "," + countSpider  + "," + googlebotNum  + "," 
										+ baiduspiderNum  + "," + baidugameNum  + "," + msnbotNum  + "," 
										+ bingbotNum + "," + ahrefsbotNum  + "," + spider360Num  + "," 
										+ nutchNum  + "," + sosospiderNum  + "," + sogou_web_spiderNum;
						FileUtil.appendStrToFile(result,Constants.RESOURCE_DIRECTORY, ArgsNetease.SPIDER_LOG);
						
						countSpider = 0L;
						spider_map.clear();
						timeClock4Spider = System.currentTimeMillis();
					}
				}
			}
			
			/*
			 * **********************************************************************************************************************
			 * 针对status-stream的判断逻辑
			 */
			if(source.equals("status-stream")){
				
				String statusStr =  input.getStringByField("status");
				if(statusStr == null){
					System.err.println("Merger bolt task - " + _context.getThisTaskIndex() 
							+ " : You got a null message of status-stream from the logAnalyzer bolt.");
				}else{
					HashMap<String, Long> tmp_map = new HashMap<String, Long>();
					tmp_map = MapTools.strToMap(statusStr);
					status_map = MapTools.addMap(status_map, tmp_map);
				}
				
				if((System.currentTimeMillis() - timeClock4Status) >= ArgsNetease.archivePeriod){
					
					if(!status_map.isEmpty()){
						long one = 0L, two = 0L, three = 0L, four = 0L, five = 0L;
						
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
						
						try{
							RemoteMySQLAccess rma = new RemoteMySQLAccess();
							
							rma.execute("insert into httpstate " +
									"(one_xx, two_xx, three_xx, four_xx, five_xx) " +
									"value ('"+one+"', '"+two+"', '"+three+"', '"+four+"', '"+five+"')");
							
						}catch(Exception ex){
							System.err.println("Error : " + ex.toString());
							 FileUtil.appendStrToFile(datef.format(new Date()) + "\tError:\t" + ex.toString(), 
									  Constants.RESOURCE_DIRECTORY, "mySQL.log");
						}
						
						// 写文件
						String result = datef.format(new Date()) + "," + one  + "," + two + "," + three + "," 
										+ four + "," + five;
						
						FileUtil.appendStrToFile(result, Constants.RESOURCE_DIRECTORY, ArgsNetease.STATUS_LOG);
						
						status_map.clear();
						timeClock4Status = System.currentTimeMillis();
					}
				}
			}
			
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
		
		this.timeClock4PV = System.currentTimeMillis();
		this.timeClock4Spider = System.currentTimeMillis();
		this.timeClock4Status = System.currentTimeMillis();
		
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
