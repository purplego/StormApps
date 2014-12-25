package twitter.stormImproved;

import java.util.HashMap;
import java.util.Map;

import twitter.tools.Args4Twitter;
import twitter.tools.ArgsTools;
import util.Constants;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagsCounterBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5509600523090071048L;
	
	private TopologyContext _context;
	private OutputCollector _collector;

	private HashMap<String, Long> tags_map;
	
	private long clock; 				// 初始值为该task首次执行的时间，其后每次向MergerBolt发送数据后会重置为当前时间
	
	public void execute(Tuple input) {
		
		String tag_str = input.getStringByField("tag");// 接收来自spout的数据流数据，FieldName为tweet
		tag_str = tag_str.toLowerCase();
		if(tag_str == null){
			System.out.println("hashtagsCounter bolt - " + _context.getThisTaskIndex() 
					+ " : Got a null message from the hashtagsSplitterBolt.");
			
		}else{
			
			long value = 0L;
			if(tags_map.containsKey(tag_str)){
				value = tags_map.get(tag_str);
				value ++;
				
			}else{
				value = 1L;
			}
			tags_map.put(tag_str, value);
			_collector.emit("tagCount-stream", new Values(tag_str, value));
			
			/*
			 * 定期清空本地的map
			 */
			if((System.currentTimeMillis() - clock) >= Args4Twitter.clearPeriod){
				
				tags_map.clear();
				clock = System.currentTimeMillis();
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
		_collector = collector;
		
		tags_map = new HashMap<String, Long>();
		clock = System.currentTimeMillis();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream("tagCount-stream", new Fields("tag", "count"));
	}

}
