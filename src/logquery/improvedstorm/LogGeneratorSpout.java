package logquery.improvedstorm;

import java.util.Map;

import util.Constants;


import logquery.tools.ArgsTools;
import logquery.tools.ArgsLogquery;
import logquery.tools.LogGenerator;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class LogGeneratorSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 8549231173565757488L;
	
	private SpoutOutputCollector _collector;
	private TopologyContext _context;
	int streamInterval = 0;
	int countInterval = 1000;
	boolean flag = true;
	
	/**
	 * The only thing that the method will do IS emitting each file line
	 */
	public void nextTuple() {

		/*
		 * The nextTuple() is called forever, so if we have been read the whole file,
		 * we will wait several seconds and then return
		 */
		String str_line = LogGenerator.recordGenerator();
		_collector.emit("log-stream", new Values(str_line));
		Utils.sleep(streamInterval);
	}

	/**
	 * 变量初始化: get the collector object and create the input log file
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_collector = collector;
		_context = context;
		
		streamInterval = ArgsLogquery.streamInterval;
		
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("log-stream", new Fields("logRecord"));
	}

}
