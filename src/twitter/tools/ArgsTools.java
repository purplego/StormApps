package twitter.tools;

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
			Args4Twitter.numWorkers = Integer.parseInt(argsParse.getArgs("numWorkers"));
		if(argsParse.isContains("numAckers"))
			Args4Twitter.numAckers = Integer.parseInt(argsParse.getArgs("numAckers"));
		
		/*
		 * Spout并行度，以及第一级、第二级、第三级Bolt的并行度
		 */
		if(argsParse.isContains("paraSpout"))
			Args4Twitter.paraSpout = Integer.parseInt(argsParse.getArgs("paraSpout"));
		if(argsParse.isContains("paraBolt1"))
			Args4Twitter.paraBolt1 = Integer.parseInt(argsParse.getArgs("paraBolt1"));
		if(argsParse.isContains("paraBolt2"))
			Args4Twitter.paraBolt2 = Integer.parseInt(argsParse.getArgs("paraBolt2"));
		if(argsParse.isContains("paraBolt3"))
			Args4Twitter.paraBolt3 = Integer.parseInt(argsParse.getArgs("paraBolt3"));
		
		/*
		 * LogAnalyzerBolt向MergerBolt发送局部统计数据的周期
		 * MergerBolt向控制台打印统计结果的周期
		 * MergerBolt将统计结果归档的周期
		 */
		if(argsParse.isContains("clearPeriod"))
			Args4Twitter.clearPeriod = Integer.parseInt(argsParse.getArgs("clearPeriod"));
		if(argsParse.isContains("archivePeriod"))
			Args4Twitter.archivePeriod = Integer.parseInt(argsParse.getArgs("archivePeriod"));
		
		/*
		 * 从文本读取数据并发送的周期
		 */
		if(argsParse.isContains("streamInterval"))
			Args4Twitter.streamInterval = Integer.parseInt(argsParse.getArgs("streamInterval"));
	}
}
