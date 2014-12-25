package wordcount.storm;

import java.util.Map;
import java.util.Random;

import util.Constants;
import wordcount.tools.ArgsWordcount;
import wordcount.tools.ArgsTools;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -2456398061201936538L;
	
	SpoutOutputCollector _collector;
	Random rand;

	public void nextTuple() {
		// TODO Auto-generated method stub
        Utils.sleep(ArgsWordcount.streamInterval);
        //字符串数组，组成多个sentence
        String[] sentences = new String[] {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"};
        String sentence = sentences[rand.nextInt(sentences.length)];
        _collector.emit("sentence-stream", new Values(sentence));
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_collector = collector;
        rand = new Random();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("sentence-stream", new Fields("sentence"));
	}

}
