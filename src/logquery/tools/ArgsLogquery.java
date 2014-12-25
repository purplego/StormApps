package logquery.tools;

public class ArgsLogquery {

	public static String ACCESS_LOG = "access_log"; 			// 原始的日志文件
	public static String PV_LOG = "statPV"; 					// PV的统计结果
	public static String UV_LOG = "statUV"; 					// UV的统计结果
	public static String STATUS_LOG = "statStatus"; 			// Status Distribution的统计结果
	public static String SPIDER_LOG = "statSpider"; 			// Spider Distribution的统计结果
	
	public static int maxTaskPara = 100;						// 设置最大线程并行度，默认为10（该参数只在本地模式下有用）
	public static int numWorkers = 5;							// 设置Worker的数目，默认为5
	public static int numAckers = 0;							// 设置Acker的数目，默认为0
	
	public static int paraSpout = 1;							// 设置Spout并行度，默认为1
	public static int paraBolt1 = 10;							// 设置第一级Bolt的并行度，默认为3
	public static int paraBolt2 = 1;							// 设置第二级Bolt的并行度，默认为1
	public static int paraBolt3 = 1;							// 设置第二级Bolt的并行度，默认为1
	
	public static int transPeriod = 10*1000;					// 设置LogAnalyzerBolt向MergerBolt发送局部统计数据的周期，默认为5分钟
	public static int printPeriod = 30*1000;					// 设置MergerBolt向控制台打印统计结果的周期，默认为5分钟
	public static int archivePeriod = 1*60*1000;				// 设置MergerBolt将统计结果归档的周期，默认为15分钟
	public static int clearPeriod = 24*60*60*1000;				// 设置LogAnalyzerBolt和MergerBolt定期清空map的周期，默认为24小时
	
	public static int streamInterval = 0;						// 从文本读取数据并发送的周期，默认为1 elements per second
}
