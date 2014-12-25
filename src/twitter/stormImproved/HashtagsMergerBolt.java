package twitter.stormImproved;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import twitter.tools.ArgsTools;
import twitter.tools.Args4Twitter;
import util.Constants;
import util.FileUtil;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HashtagsMergerBolt extends BaseRichBolt {

	private static final long serialVersionUID = -4842035105601637714L;
	
	private TopologyContext _context;
	
	private HashMap<String, Long> tags_map;
//	private long clock;
	
	/**
	 * 
	 */
	public void execute(Tuple input) {
		
		String source = input.getSourceStreamId();
		
		if(source.equals("tagCount-stream")){
			
			String tag = input.getStringByField("tag");
			long count = input.getLongByField("count");
			
			if(tags_map.containsKey(tag)){
				count += tags_map.get(tag);
			}
			tags_map.put(tag, count);
			
		}else if(source.equals("signal")){
			
			String signal = input.getStringByField("endOfFile");
			if(signal.equals("endOfFile")){
				
				/*
				 * 如果收到endOfFile信号，则将数据归档
				 */
				if(!tags_map.isEmpty()){
//					SimpleDateFormat datef = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
//					FileUtil.appendStrToFile(datef.format(new Date()), Constants.RESOURCE_DIRECTORY, Args4Twitter.HashTags_LOG);
					
					Set<String> keySet = tags_map.keySet();
					for(String key: keySet){
						
						String str = key + "," + tags_map.get(key);
						FileUtil.appendStrToFile(str, Constants.RESOURCE_DIRECTORY, Args4Twitter.HashTags_LOG);
					}
					tags_map.clear();
				}
			}
			
		}
		
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		/*
		 * 变量初始化
		 */
		_context = context;
		
		tags_map = new HashMap<String, Long>();
//		clock = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
