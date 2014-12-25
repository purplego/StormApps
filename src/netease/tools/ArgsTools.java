package netease.tools;

import util.ArgsParse;

public class ArgsTools {

	/**
	 * the parseArgs approach: to parse args in config file
	 * @param dir 			目标文件所在的文件夹
	 * @param config_file 	目标文件的文件名
	 */
	public static void parseArgs(String config_dir, String config_file){
		
		ArgsParse argsParse = new ArgsParse(config_dir, config_file);
		
		/*
		 * 运行该Topology的WorkerNum和AckerNum
		 */
		if(argsParse.isContains("numWorkers"))
			ArgsNetease.numWorkers = Integer.parseInt(argsParse.getArgs("numWorkers"));
		if(argsParse.isContains("numAckers"))
			ArgsNetease.numAckers = Integer.parseInt(argsParse.getArgs("numAckers"));
		
		/*
		 * Spout并行度，以及第一级、第二级、第三级Bolt的并行度
		 */
		if(argsParse.isContains("paraSpout"))
			ArgsNetease.paraSpout = Integer.parseInt(argsParse.getArgs("paraSpout"));
		if(argsParse.isContains("paraBolt1"))
			ArgsNetease.paraBolt1 = Integer.parseInt(argsParse.getArgs("paraBolt1"));
		if(argsParse.isContains("paraBolt2"))
			ArgsNetease.paraBolt2 = Integer.parseInt(argsParse.getArgs("paraBolt2"));
		
		/*
		 * LogAnalyzerBolt向MergerBolt发送局部统计数据的周期
		 * MergerBolt向控制台打印统计结果的周期
		 * MergerBolt将统计结果归档的周期
		 */
		if(argsParse.isContains("archivePeriod"))
			ArgsNetease.archivePeriod = Integer.parseInt(argsParse.getArgs("archivePeriod"));
		
		/*
		 * 是否以变化速率发送数据流，如果是，数据流速度变化周期为多少
		 */
		if(argsParse.isContains("isVaring"))
			ArgsNetease.isVaring = Boolean.parseBoolean(argsParse.getArgs("isVaring"));
		if(argsParse.isContains("varingPeriod"))
			ArgsNetease.varingPeriod = Integer.parseInt(argsParse.getArgs("varingPeriod"));
		
		/*
		 * 从文本读取/直接生成数据并发送的周期
		 */
		if(argsParse.isContains("streamInterval"))
			ArgsNetease.streamInterval = Integer.parseInt(argsParse.getArgs("streamInterval"));
	}
}
