package util;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RemoteMySQLAccess {
	
	public Connection 	conn;  		// 用于建立到MySQL数据库的连接 
	public Statement 	stmt;  		// 用来执行SQL语句
	SimpleDateFormat datef = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
	
	/**
	 * RemoteMySQLAccess类的构造函数
	 */
	public RemoteMySQLAccess(){
		
		// String url="jdbc:mysql://192.168.1.2/"+ dbName + "?useUnicode=true&characterEncoding=GB2312";
		
		/*
		 * 驱动程序名
		 * URL指向要访问的数据库名(cloud)，即数据源"jdbc:mysql://10.107.18.202:3306/storm"
		 * MySQL配置时的用户名和密码
		 */
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://" + Constants.DB_SERVER + ":" + Constants.DB_PORT + "/" + Constants.DB_NAME;
		String user = Constants.USER;
		String password = Constants.PASSWORD;
		
		try {  

			Class.forName(driver).newInstance();  						// 加载ConnectorJ驱动 ，也可写为Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);	// 建立到MySQL数据库的连接 
			
			if(!conn.isClosed()){
				System.out.println(datef.format(new Date()) + "\tSucceeded connecting to the Database!");
			}
			
			stmt = conn.createStatement();  								// statement用来执行SQL语句
		  
		}catch (Exception ex){  
		  System.err.println("Error : " + ex.toString());  
		  FileUtil.appendStrToFile(datef.format(new Date()) + "\tError when create a connection\t" + ex.toString(), 
				  Constants.RESOURCE_DIRECTORY, "mySQL.log");
		}
	}
	
	/**
	 * 实际执行一条SQL语句
	 * @param sql 待执行的SQL语句
	 */
	public void execute(String sql){
		try{
			
			stmt.execute(sql);
//			conn.close();
			
		}catch (Exception ex){  
		  System.err.println("Error : " + ex.toString());  
		  FileUtil.appendStrToFile(datef.format(new Date()) + "\tError when execute a sql\t" + ex.toString(), 
				  Constants.RESOURCE_DIRECTORY, "mySQL.log");
		}
	}
}
