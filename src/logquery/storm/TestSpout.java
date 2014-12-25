package logquery.storm;

import java.util.Map;

import util.Constants;

import logquery.tools.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 8549231173565757488L;
	
	private SpoutOutputCollector _collector;
	private TopologyContext _context;
	
	long starttime, endtime;
	int count;
	int totalNum = 20000;
	
	/**
	 * The only thing that the method will do IS emitting each file line
	 */
	public void nextTuple() {

		/*
		 * The nextTuple() is called forever, so if we have been read the whole file,
		 * we will wait several seconds and then return
		 */
		Utils.sleep(ArgsLogquery.streamInterval);
//		if(count < totalNum){
			String str_line = LogGenerator.recordGenerator();
			_collector.emit("log-stream", new Values(str_line));
			count ++;
//		}else{
//			endtime = System.currentTimeMillis();
//			System.out.println("------" + (endtime - starttime) + " ------ count = " + count);
//			Utils.sleep(60*1000);
//			return;
//		}
	}

	/**
	 * 变量初始化: get the collector object and create the input log file
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_collector = collector;
		_context = context;
		
		starttime = System.currentTimeMillis();
		endtime = System.currentTimeMillis();
		
		count = 0;
		
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("log-stream", new Fields("logRecord"));
	}

}
