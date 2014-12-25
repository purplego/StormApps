package twitter.filespilt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import util.Constants;

public class TimeStatistic {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		long totalProcTime = 0L;
		long totalTweetNum = 0L;
		/*
		 * 读取文件并提取出处理时间和tweetNum的信息
		 */
		String pathIn = Constants.RESOURCE_DIRECTORY + "note.txt";
		File fileIn = new File(pathIn);
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(fileIn));
			
			String str_line = "";
			while(bReader.ready()){
				str_line = bReader.readLine();
				if(str_line != null){
					
					if(str_line.startsWith("Finished to process")){
						
						String[] marks = str_line.split(":");
						marks[1] = marks[1].replace("ms.", "");
						marks[1] = marks[1].replace(" ", "");
						totalProcTime += Long.parseLong(marks[1]);
						
					}else if(str_line.startsWith("Total tweet number")){
						
						String[] marks = str_line.split(":");
						marks[1] = marks[1].trim();
						totalTweetNum += Long.parseLong(marks[1]);
					}
						
				}
			}
			bReader.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long day = totalProcTime / (1000*60*60*24);
		long hour = (totalProcTime % (1000*60*60*24)) / (1000*60*60);
		long minute = ((totalProcTime % (1000*60*60*24)) % (1000*60*60)) / (1000*60);
		long second = (((totalProcTime % (1000*60*60*24)) % (1000*60*60)) % (1000*60)) / 1000;
		long ms = (((totalProcTime % (1000*60*60*24)) % (1000*60*60)) % (1000*60)) % 1000;
		
		System.out.println("Total process time: " + totalProcTime + " ms, that is: " + 
				day + " days " + hour + " hours " + minute + " minutes " + second + " seconds " + ms + " ms.");
		System.out.println("Total tweet Number: " + totalTweetNum + " .");
	}

}
