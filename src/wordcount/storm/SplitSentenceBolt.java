package wordcount.storm;

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

public class SplitSentenceBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5506782579135366136L;
	OutputCollector _collector;

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String sentence = input.getStringByField("sentence");
		for(String word: sentence.split(" ")){
			_collector.emit("word-stream", new Values(word));
		}
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("word-stream", new Fields("word"));
	}

}
