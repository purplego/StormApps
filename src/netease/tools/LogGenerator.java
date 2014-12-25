package netease.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class LogGenerator {

	public static void main(String[] args) {
		
		long starttime = System.currentTimeMillis();
		long card = 100000L;
//		recordWrite(card, 0);		// 覆盖写netEaseData_10这个文件，当card改变时，写入新的文件
		recordWrite(card, 1);		// 追加写netEaseData这个文件
		System.out.println("Complete! \nDuring Time: " + (System.currentTimeMillis() - starttime) + " s.");
	}
	
	/**
	 * 获取生成的数据文件的文件名，表示生成了多少条记录
	 * @return
	 */
	public static String genCard(Long size){
		String str_card = ""; 
		if(size >= 1000000000)
			str_card = (size/1000000000 + "G");
		else if(size >= 1000000)
			str_card = (size/1000000 + "M");
		else if(size >= 1000)
			str_card = (size/1000 + "K");
		else
			str_card = (size + "");;
		return str_card;
	}
	
	/**
	 * 按照指定格式生成一条日志记录
	 * @return
	 */
	public static String recordGenerator(){
		/*
		 * 一条记录的各个属性域可能取到的值
		 */
		String[] cookieSet = { "ZUcIg1Jl42UN1RxlUAV4Ag==", "-", 
				"ZUcIg1J26mCA/35+IlWqAg==", "ezq041IwHL1HHgSCAwNRAg==",
				"ezq05FI1HecstEorPZPJAg==", "ZUcIg1KAcRxFZCJRaSy/Ag==",
				"ZUcIhFKTIfqManqjBAGQAg==", "ezq0JFIxWWyBwB6XFFtZAg==",
				"ZUcIhFKSoT67+hIDM2w+Ag==", "ZUcIg1KTJZcs11eUBC9xAg==",
				"ezq0JFIwHuNC+iEUAxvBAg==","ezq0JVIxp56QmzLNBNQEAg==",
				"ZUcIhFKTI9mDT3qJBAmzAg==","ZUcIhFJc47q192oRA+8QAg==",
				"ZUcIhFJZTfJ82CNxIJIHAg==" };
		
		String[] requestSet = { "GET /Q.tYx HTTP/1.1", 
				"GET /XxOOxE/9F9.s?Nxkv=k7DOoga5_&2=B HTTP/1.1",
				"GET /XxOOxE/9F9.s?Nxkv=Au9ED9EQA3&2=B&v=rrB HTTP/1.1",
				"GET /XxOOxE/9F9.s?Nxkv=NDQYsi&2=B HTTP/1.1",
				"HEAD /2QxD/kv9vuX/rgbBe44gaBg5Sg4agBlab/ HTTP/1.1", 
				"POST /Y9EDY9EAuEBlgS/KYs/X9QQ/JQ9uEX9QQ/dQxDdU9E1UY.DUvhukvxsodQxDkVET9OUG9o.dwr HTTP/1.1",
				"GET /XxOOxE/9F9.s?Nxkv=sfOUEDrr5B@gar&2=B HTTP/1.1", 
				"POST /3Dofrr/KYs/X9QQ/JQ9uEX9QQ/CkUsdU9E1UY.fJK9vUCkUsyEQuEUTv9vfk.dwr HTTP/1.1", 
				"GET /XxOOxE/9F9.s?Nxkv=oA3elBa&2=B HTTP/1.1", 
				"GET /9Ju/OkD/DUv?kuK=XdjCqPwwzuXmJRcKZCwwRiMhcPqv7n3y&fuK=NfuoukA3@gr5.XxO&Nxkv" +
				"=XYU2O9uQ.O9uQ.gr5.XxO&FUs=tke&kvoQU=g&kiuE=tK2&XxQxs=BB55bb&_KxXvoJU=tk&_X9QQ29Xi" +
				"=EUvU9kU.O9uQ.29kU.ct9A.J9KKuEDg5aSbBBbl5e5e HTTP/1.1",
				"GET /Ussxs.do?XxKU=SBS HTTP/1.0" 
				};
		
		String[] hostnameSet = { "JJ.2QxD.gr5.XxO", "xk.2QxD.gr5.XxO", 
				"EUsF5A5.2QxD.gr5.XxO", "NfOuErSBl.2QxD.gr5.XxO", 
				"JoDKggB.2QxD.gr5.XxO", "tQXNfE.2QxD.gr5.XxO", 
				"Ptu9EKxED.2QxD.gr5.XxO", "tu9EDPufofagl.2QxD.gr5.XxO",
				"NviQlBga.2QxD.gr5.XxO", "7N9EDNxEDsUFuUY.2QxD.gr5.XxO" };
		
		String[] httpstateSet = { "100","100","100","100", 
									"200", "200", "200", "200", "200", "200", "200","200","200",
									"301", "302", "304", "301", "302", "304",
									"403", "404", "405", 
									"500"};
		
		String[] refererSet = { "-", "http://ofo9Qu9EDaBBe.2QxD.gr5.XxO/", 
				"http://2QxD.gr5.XxO/Ok_ktuED/9Q2fO/", 
				"http://9FUvxu.OUQxKo.bg4.2QxD.gr5.XxO/9Q2fO/?EUYMxQQxYdQxD", 
				"http://7Not-aBBl.2QxD.gr5.XxO/"};
		
		String[] agentSet = {  
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; " +
				"Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; " +
				".NET CLR 3.0.30729; .NET4.0E; .NET4.0C)", 
				"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)", 
				"Python-urllib/2.7 AppEngine-Google; (+http://code.google.com/appengine; appid: s~xneteasegoblin)",
				"-","Mozilla/5.0 (Linux; U; Android 2.3.3; zh-cn; HTC Sensation Z710e Build/GRI40) " +
				"AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1 MicroMessenger/5.0.351",
				"Opera/9.80 (Windows NT 6.2; WOW64; SimpleU Edition) Presto/2.12.388 Version/12.15",
				"Mozilla/4.0 (compatible; MSIE 8.0; YYGameAll_1.2.167057.92; Windows NT 5.1; Trident/4.0; " +
				"qdesk 2.4.1264.203; QQDownload 718)",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ", 
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; ",
				
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; BaiduGame)",
				"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
				"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
				"msnbot/2.0b (+http://search.msn.com/msnbot.htm)",
				"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
				"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)",
				"Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; )  Firefox/1.5.0.11; 360Spider",
				"Nutch12/Nutch-1.2",
				"Mozilla/5.0 (compatible; Sosospider/2.0; +http://help.soso.com/webspider.htm)",
				"Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)"
				};
		
		/*
		 *生成一条记录的各个属性域
		 */

		Random random = new Random();
		
		DecimalFormat decimalf = new DecimalFormat("#.###");
		String response = "\"response\":" + "\"" + decimalf.format(random.nextDouble()) + "\"";
		
		String cookie = "\"cookie\":" + "\"" + cookieSet[random.nextInt(cookieSet.length)] + "\"";
		
		SimpleDateFormat datef = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		String timestamp = "\"timestamp\":" + "\"" + datef.format(new Date()) + "\"";
		
		String request = "\"request\":" + "\"" + requestSet[random.nextInt(requestSet.length)] + "\"";

		String hostName = "\"hostName\":" + "\"" + hostnameSet[random.nextInt(hostnameSet.length)] + "\"";

		String httpstate = "\"httpstate\":" + "\"" + httpstateSet[random.nextInt(httpstateSet.length)] + "\"";

		String referer = "\"referer\":" + "\"" + refererSet[random.nextInt(refererSet.length)] + "\"";

		String agent = "\"agent\":" + "\"" + agentSet[random.nextInt(agentSet.length)] + "\"";

		String size = "\"size\":" + "\"" + random.nextInt(1000000) + "\"";

		String ip = "\"ip\":" + "\"" + ((random.nextInt(255)+1) + "."+random.nextInt(256)
				+ "." + random.nextInt(256) + "." + random.nextInt(256)) + "\"";

		String record = "{" + response + "," + cookie + "," + timestamp + "," + request + "," 
			+ hostName + "," + httpstate + "," + referer + "," + agent + "," + size + "," + ip + "}";
		
		return record;
	}
	
	/**
	 * 把生成的日志记录字符串写入文档，
	 * 写入中文字符时解决中文乱码问题
	 * @param mode 写入文件的方式：0为覆盖写，1为追加写
	 */
	public static void recordWrite(long cardinality, int mode){
		
		String card = genCard(cardinality);									//生成多少条记录
		String filePath = "./resource/netEaseData_" + card;					//日志写在哪里
		String filePath2 = "./resource/netEaseData";
		
		try {
			FileWriter fw = null;
			switch(mode){
			case 0:
				fw = new FileWriter(new File(filePath));					// 覆盖写
				break;
			case 1:
				fw = new FileWriter(new File(filePath2), true);				// 追加写
				break;
			default:
				System.out.println("Error Mode value. Please Specify 0 or 1.");
				break;	
			}
			
			for(int i=0; i<cardinality; i++){
				fw.write(recordGenerator() + "\n");
				fw.flush();
			}
			// 注意关闭的先后顺序，先打开的后关闭，后打开的先关闭
			fw.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
}