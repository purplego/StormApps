package twitter.filespilt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;


import util.Constants;

public class PostProcess {

	/**
	 * 对twitter数据的后处理
	 * 已经有一些局部结果存在hashtags.csv中，读取该文件并聚合相同key的项，生成一个新文件finalStat.csv
	 * @param args
	 */
	public static void main(String[] args) {

		long starttime = System.currentTimeMillis();
		HashMap<String, Integer> tag_map = new HashMap<String, Integer>();
		/*
		 * 读取文件并构建成一个Map
		 */
		String pathIn = Constants.RESOURCE_DIRECTORY + "hashtags1.csv";
		File fileIn = new File(pathIn);
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(fileIn));
			
			String str_line = "";
			while(bReader.ready()){
				str_line = bReader.readLine();
				if((str_line != null) && (!str_line.startsWith("2014-"))){
					
					String[] marks = str_line.split(",");
					
					int value = Integer.parseInt(marks[1]);
					if(tag_map.containsKey(marks[0])){
						
						value += tag_map.get(marks[0]);
					}
					tag_map.put(marks[0], value);
				}
				
			}
			bReader.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Reconstruct the map: " + (System.currentTimeMillis() - starttime) + " ms.");
		starttime = System.currentTimeMillis();
		/*
		 * 将Map写入文件
		 */
		String pathOut = Constants.RESOURCE_DIRECTORY + "finalStat.csv";
		File fileOut = new File(pathOut);
		try{
			BufferedWriter bWriter = new BufferedWriter(new FileWriter(fileOut));
			Set<String> keySet = tag_map.keySet();
			for(String key: keySet){
				
				String str = key + "," + tag_map.get(key);
				bWriter.write(str);
				bWriter.newLine();
				bWriter.flush();
			}
			
			bWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Write the map: " + (System.currentTimeMillis() - starttime) + " ms.");
	}

}
