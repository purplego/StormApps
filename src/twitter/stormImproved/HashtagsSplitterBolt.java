package twitter.stormImproved;

import java.util.Map;

import org.json.simple.*;

import twitter.tools.ArgsTools;
import util.Constants;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagsSplitterBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2847567867301983084L;
	
	private TopologyContext _context;
	private OutputCollector _collector;

	
	public void execute(Tuple input) {
		
		String tweetStr = input.getStringByField("tweet");// 接收来自spout的数据流数据，FieldName为tweet
		if(tweetStr == null){
			System.out.println("hashtagsSplitter bolt - " + _context.getThisTaskIndex() 
					+ " : Got a null message from the spout.");
			
		}else{
			
			/*
			 * 将接收到的字符串解析为JSON对象，
			 * 找出英语语言的tweet，即符合"lang":"en"的记录
			 * 如果"lang"不为"en"，或者"lang"为null，则跳过本条tweet消息
			 */
			Object obj = JSONValue.parse(tweetStr);
			JSONObject tweetObj = (JSONObject)obj;
			
			if(tweetObj != null){
				if((tweetObj.get("lang")) != null){
					if(tweetObj.get("lang").equals("en")){
						
						JSONObject entitiesObj = (JSONObject)tweetObj.get("entities");
						if(!entitiesObj.isEmpty()){
							
							JSONArray hashtagsArray = (JSONArray)entitiesObj.get("hashtags");
							if(!hashtagsArray.isEmpty()){
								
								for(int index=0; index<hashtagsArray.size(); index++){
									JSONObject textObj = (JSONObject)hashtagsArray.get(index);
									String tag = (String)textObj.get("text");
									_collector.emit("tags-stream", new Values(tag));
								}
							}else{
								// Do nothing
							}
						}else{
							// Do nothing
						}
						
					}else{
//						System.err.println("The language of the tweetObj is not Engish.");
					}
					
				}else{
//					System.err.println("The lang field of the tweetObj is null.");
//					System.err.println(tweetStr);
				}
				
			}else{
				System.err.println("The tweetObj is null.");
				System.out.println(tweetStr);
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
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream("tags-stream", new Fields("tag"));
	}

}
