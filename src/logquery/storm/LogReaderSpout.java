package logquery.storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import util.Constants;

import logquery.tools.ArgsTools;
import logquery.tools.ArgsLogquery;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class LogReaderSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 8549231173565757488L;
	
	private SpoutOutputCollector _collector;
	private TopologyContext _context;
	
	long starttime, starttime1,endtime;
	
	/*
	 * 与源文件读取相关的一些变量
	 */
	private boolean 	endOfFile = false;							// 判断是否到了文件末尾
	private String 		filePathIn = Constants.RESOURCE_DIRECTORY + ArgsLogquery.ACCESS_LOG;
	private FileReader 	fReader;

	/**
	 * The only thing that the method will do IS emitting each file line
	 */
	public void nextTuple() {

		/*
		 * The nextTuple() is called forever, so if we have been read the whole file,
		 * we will wait several seconds and then return
		 */
		if(endOfFile){
			try {
				Thread.sleep(10*1000);
			} catch (InterruptedException e) {
				// DO nothing
			}
			return;
		}
		
		/*
		 * 逐行读取文件，且每读取一行即向下一级Bolt发送数据
		 */
		BufferedReader bReader = new BufferedReader(fReader); 				// Open the BufferedReader
		try{
			starttime1 = System.currentTimeMillis();
			while(bReader.ready()){
				String str_line = bReader.readLine().trim();				// Read a line from the file
				
				if((str_line == null)){
					System.err.println("Spout task - " + _context.getThisTaskIndex() 
							+ ": Reading an empty line in the source file.");
				}else if(str_line.equals("{}")){
					System.err.println("Spout task - " + _context.getThisTaskIndex() 
							+ ": A {} line.");
				}else{
					_collector.emit("log-stream", new Values(str_line));
					Utils.sleep(ArgsLogquery.streamInterval);					// One element per second in default
				}
			}
			endtime = System.currentTimeMillis();
			System.out.println("------" + (endtime - starttime));
			System.out.println("++++++" + (endtime - starttime1));
			bReader.close();
			fReader.close();
		}catch(Exception e){
			throw new RuntimeException("Spout task - " + _context.getThisTaskIndex() 
					+ " : Error reading a line.");
		}finally{
			endOfFile = true;
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
		
		starttime = System.currentTimeMillis();
		starttime1 = System.currentTimeMillis();
		endtime = System.currentTimeMillis();
		
		try {
			fReader = new FileReader(new File(filePathIn));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Spout task - " + _context.getThisTaskIndex() 
					+ " : Error reading a source file: " + filePathIn);
		}
	}

	/**
	 * Declare the output filed, i.e., the output stream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("log-stream", new Fields("logRecord"));
	}

}
