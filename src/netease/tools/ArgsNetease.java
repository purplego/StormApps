package netease.tools;

public class ArgsNetease {

	public static String ACCESS_LOG = "access_log"; 			// 原始的日志文件
	public static String PV_LOG = "statPV.csv"; 				// PV的统计结果
	public static String UV_LOG = "statUV.csv"; 				// UV的统计结果
	public static String STATUS_LOG = "statStatus.csv"; 		// Status Distribution的统计结果
	public static String SPIDER_LOG = "statSpider.csv"; 		// Spider Distribution的统计结果
	
	public static int maxTaskPara = 100;						// 设置最大线程并行度，默认为10（该参数只在本地模式下有用）
	public static int numWorkers = 10;							// 设置Worker的数目，默认为5
	public static int numAckers = 0;							// 设置Acker的数目，默认为0
	
	public static int paraSpout = 1;							// 设置Spout并行度，默认为1
	public static int paraBolt1 = 20;							// 设置第一级Bolt的并行度，默认为3
	public static int paraBolt2 = 1;							// 设置第二级Bolt的并行度，默认为1
	public static int numTask = 1;								// 设置每个executor起多少个task，默认为1
	
	public static int archivePeriod = 10*1000;					// 设置LogAnalyzerBolt向MergerBolt发送局部统计数据的周期，以及MergerBolt将统计结果归档的周期，默认为10s
	
	public static boolean isVaring = false;						// 是否以变化速率发送数据流，默认为false，即以恒定速率发送数据流
	public static int varingPeriod = 120*1000;					// 数据流发送速率发生变化的周期，默认为20秒
	
	public static int streamInterval = 10;						// 从文本读取数据并发送的周期，默认为1 elements per second
	
}
