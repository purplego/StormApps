package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.*;

import util.FileUtil;


public class TestJson {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

//		System.out.println("\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412\ue412");
//		System.out.println("@vegpux \u4eca\u65e5\u304b\u3089\u304b\u30fb\u30fb\u30fb\u9811\u5f35\u308c(*\uff9f\u25bd\uff9f)\uff89");
		
//		System.out.println(FileUtil.getLinesFromFile(1, "//10.107.18.201/data/", "statuses.log.2013-03-18-16"));
		
		System.out.println("start to test JSON ......");
		
		String pathIn = "//10.107.18.201/data/" + "statuses.log.2013-03-18-16";
		File fileIn = new File(pathIn);
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(fileIn));
			
			String str = "";
			while(bReader.ready()){
				str = bReader.readLine();
				if(str != null){
					
					/*
					 * 去掉属性为delete的tweet
					 */
					if(str.startsWith("{\"created_at\":")){
						
						Object obj = JSONValue.parse(str);
						JSONObject tweetObj = (JSONObject)obj;
						if(tweetObj != null){
							
							/*
							 * 找出语言属性不为空且等于英语的tweet，即"lang"不等于null且"lang"等于"en"
							 */
							if(tweetObj != null){
								
								if(tweetObj.get("lang") != null){
									
									if(tweetObj.get("lang").equals("en")){
										
										/*
										 * ***************** 将每一条tweet中的hashtags找出来并进行分解 *****************
										 */
										
										JSONObject entitiesObj = (JSONObject)tweetObj.get("entities");
										if(!entitiesObj.isEmpty()){
											
											JSONArray hashtagsArray = (JSONArray)entitiesObj.get("hashtags");
											if(!hashtagsArray.isEmpty()){
												// 输出整个Hashtags
//												System.out.println("\tHashtags\t" + hashtagsArray);
												// 输出Hashtags中的标签文本(text)
												for(int index=0; index<hashtagsArray.size(); index++){
													JSONObject textObj = (JSONObject)hashtagsArray.get(index);
													String text = (String)textObj.get("text");
//													System.out.println(text);
												}
											}else{
//												System.out.println("The hashtagsArray is empty.");
											}
										}else{
//											System.out.println("The entitiesObj is empty.");
										}
										
										/*
										 * *********************** 处理tweet的正文部分，即处理tweet的"text"属性 ***********************
										 */
//										String text = (String)tweetObj.get("text");
										
										// 去掉text中的回车符
//										text = text.replace("\n", " ");
										
										// 去掉text中的http开头的网址（去除URL）
										
//										System.out.println("\tText\t\t" + text);
										
										
									}else{
//										System.out.println("This tweet is not in English.");
									}
									
								}else{
//									System.err.println("The lang field of the tweetObj is null.");
//									System.err.println(str);
								}
								
							}
						}else{
							
							System.err.println("The tweetObj is null.");
							System.out.println(str);
						}
					}
					
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
