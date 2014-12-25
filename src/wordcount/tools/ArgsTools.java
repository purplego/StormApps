package wordcount.tools;

import util.ArgsParse;

public class ArgsTools {

	/**
	 * the parseArgs approach: to parse args in config file
	 * @param dir 			目标文件所在的文件夹
	 * @param config_file 	目标文件的文件名
	 */
	public static void parseArgs(String dir, String config_file){
		
		ArgsParse argsParse = new ArgsParse(dir, config_file);
		
		/*
		 * 运行该Topology的WorkerNum和AckerNum
		 */
		if(argsParse.isContains("numWorkers"))
			ArgsWordcount.numWorkers = Integer.parseInt(argsParse.getArgs("numWorkers"));
		if(argsParse.isContains("numAckers"))
			ArgsWordcount.numAckers = Integer.parseInt(argsParse.getArgs("numAckers"));
		
		/*
		 * Spout并行度，以及第一级、第二级、第三级Bolt的并行度
		 */
		if(argsParse.isContains("paraSpout"))
			ArgsWordcount.paraSpout = Integer.parseInt(argsParse.getArgs("paraSpout"));
		if(argsParse.isContains("paraBolt1"))
			ArgsWordcount.paraBolt1 = Integer.parseInt(argsParse.getArgs("paraBolt1"));
		if(argsParse.isContains("paraBolt2"))
			ArgsWordcount.paraBolt2 = Integer.parseInt(argsParse.getArgs("paraBolt2"));
		
		/*
		 * 从文本读取数据并发送的周期
		 */
		if(argsParse.isContains("streamInterval"))
			ArgsWordcount.streamInterval = Integer.parseInt(argsParse.getArgs("streamInterval"));
	}
}
