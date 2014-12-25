package wordcount.tools;

public class ArgsWordcount {

	public static int maxTaskPara = 100;						// 设置最大线程并行度，默认为10（该参数只在本地模式下有用）
	public static int numWorkers = 5;							// 设置Worker的数目，默认为5
	public static int numAckers = 0;							// 设置Acker的数目，默认为0
	
	public static int paraSpout = 8;							// 设置Spout并行度，默认为1
	public static int paraBolt1 = 10;							// 设置第一级Bolt的并行度，默认为3
	public static int paraBolt2 = 5;							// 设置第二级Bolt的并行度，默认为1
	
	public static int streamInterval = 1;						// 从文本读取数据并发送的周期，默认为1 elements per second
}
