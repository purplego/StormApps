package netease.stormImproved;

import java.util.Map;

import util.Constants;

import netease.tools.ArgsNetease;
import netease.tools.ArgsTools;
import netease.tools.LogGenerator;

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
	
	private boolean flag;		// 控制是否该变换数据流速率
	private long clock;			// 控制时间，看是否到达一次变换周期，默认为Constants.varingPeriod
	private long controller; 	// 控制应该以哪一种streamInterval来执行数据流发送
	private int streamInterval;	// 数据流发送周期，默认等于Constants.streamInterval
	
	/**
	 * The only thing that the method will do IS emitting each file line
	 */
	public void nextTuple() {

		/*
		 * The nextTuple() is called forever, so if we have been read the whole file,
		 * we will wait several seconds and then return
		 */
		
		if(ArgsNetease.isVaring){
			
			flag = true;
			clock = System.currentTimeMillis();
			controller ++;

			if(controller % 2 == 0){
				streamInterval = ArgsNetease.streamInterval;
				while(flag){
					
					String str_line = LogGenerator.recordGenerator();
					_collector.emit("log-stream", new Values(str_line));
					Utils.sleep(streamInterval);
					
					if((System.currentTimeMillis() - clock) >= ArgsNetease.varingPeriod)
						flag = false;
				}
			}
			else{
				streamInterval = 0;
				while(flag){
					
					String str_line = LogGenerator.recordGenerator();
					_collector.emit("log-stream", new Values(str_line));
					
					if((System.currentTimeMillis() - clock) >= ArgsNetease.varingPeriod)
						flag = false;
				}
			}
			
		}else{
			
			String str = LogGenerator.recordGenerator();
			_collector.emit("log-stream", new Values(str));
			Utils.sleep(ArgsNetease.streamInterval);
		}
		
	}

	/**
	 * 变量初始化: get the collector object and create the input log file
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		ArgsTools.parseArgs(Constants.CONFIG_DIRECTORY, Constants.TOPOLOGY_ARGS);		// parse args and reset the Constants class
		
		_collector = collector;
		_context = context;
		
		this.flag = true;
		this.clock = System.currentTimeMillis();
		this.controller = 1L;
		this.streamInterval = ArgsNetease.streamInterval;
		
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("log-stream", new Fields("logRecord"));
	}

}
