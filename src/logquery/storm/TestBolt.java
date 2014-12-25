package logquery.storm;

import java.util.Map;

import util.Constants;

import logquery.tools.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TestBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 8963886289234350643L;
	
	private TopologyContext _context;
	

	public void execute(Tuple input) {
		String record_str = input.getStringByField("logRecord");// 接收来自spout的数据流数据，FiledName为logRecord
		if(record_str == null){
			System.out.println("logAnalyzer bolt - " + _context.getThisTaskIndex() 
					+ " : Got a null message from the spout.");
			
		}else{
			// Do nothing
		}
	}


	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		/*
		 * 变量初始化
		 */
		_context = context;
		
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
