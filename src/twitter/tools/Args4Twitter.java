package twitter.tools;

public class Args4Twitter {

	public static String TWITTER_LOG = "twitter_log"; 				// 原始的日志文件
	public static String HashTags_LOG = "hashtags.csv"; 		// Hashtags的统计结果
	
	public static int maxTaskPara = 100;						// 设置最大线程并行度，默认为10（该参数只在本地模式下有用）
	public static int numWorkers = 10;							// 设置Worker的数目，默认为5
	public static int numAckers = 0;							// 设置Acker的数目，默认为0
	
	public static int paraSpout = 1;							// 设置Spout并行度，默认为1
	public static int paraBolt1 = 5;							// 设置第一级Bolt的并行度，默认为3
	public static int paraBolt2 = 5;							// 设置第二级Bolt的并行度，默认为1
	public static int paraBolt3 = 1;							// 设置第三级Bolt的并行度，默认为1
	public static int numTask = 1;								// 设置每个executor起多少个task，默认为1
	
	public static int clearPeriod = 10*1000;					// HashtagsCounterBolt定期清空本地map的周期
	public static int archivePeriod = 10*1000;					// 设置HashtagsCounterBolt向HashtagsMergerBolt发送局部统计数据的周期，
	
	public static int streamInterval = 1;						// 从文本读取数据并发送的周期，默认为1 elements per second
	
	
}
