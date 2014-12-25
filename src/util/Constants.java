package util;

public class Constants {
	
	/*
	 * 与工作目录设置相关的参数
	 */
//	public static String CONFIG_DIRECTORY = "/home/purple/ElasticStorm/config/";
//	public static String RESOURCE_DIRECTORY = "/home/purple/ElasticStorm/resource/";
	
	public static String CONFIG_DIRECTORY = "./config/"; 		// 设置配置文件所在的目录
	public static String RESOURCE_DIRECTORY = "./resource/"; 	// 设置源数据文件所在的目录
	
	public static String TOPOLOGY_ARGS = "topoArgs"; 			// 与Topology相关的各项配置参数，在CONFIG_DIR下
	
	/*
	 * 与MySQL数据库相关的参数
	 */
	public static String DB_SERVER = "10.107.18.202";			// MySQL数据库所在的服务器地址 
	public static String DB_PORT = "3306";						// 连接MySQL数据库的端口
	public static String DB_NAME = "storm";						// 所使用的数据库名称
	public static String USER = "admin";						// MySQL配置时的用户名
	public static String PASSWORD = "admin";					// MySQL配置时的密码

}
