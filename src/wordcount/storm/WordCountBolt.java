package wordcount.storm;

import java.util.HashMap;
import java.util.Map;

import util.Constants;
import wordcount.tools.ArgsTools;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 757734092393959000L;
	OutputCollector _collector;
	Map<String, Integer> counts = new HashMap<String, Integer>();
	
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        String word = input.getStringByField("word");//Returns the String at position i in the tuple
        int count = 0;
        if(counts.containsKey(word)){
        	count = counts.get(word);
        	count++;
        }else count = 1;
        counts.put(word, count);
        _collector.emit("wordCount-stream", new Values(word, count));
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("wordCount-stream", new Fields("word", "count"));
	}

}
