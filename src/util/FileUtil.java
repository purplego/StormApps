package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

public class FileUtil {
	// ************************************************ file writers **********************************************
	// ************************************************************************************************************
	/**
	 *  把一个String类型的链表list写入dir目录下的filename文件中
	 *  
	 *  @param list 		待写入的链表 
	 *  @param dir 			目标文件所在的文件夹
	 *  @param filename 	目标文件的文件名
	 */
	public static void writeListToFile(LinkedList<String> list, String dir, String filename){
		
		String filePathOut = dir + filename;
		File fileOut = new File(filePathOut);
		try{
			FileWriter fWriter = new FileWriter(fileOut);
			BufferedWriter bWriter = new BufferedWriter(fWriter);
			for(String str: list){
				bWriter.write(str);
				bWriter.newLine();
				bWriter.flush();
			}
			bWriter.close();
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	
	/**
	 *  把一个String写入dir目录下的filename文件中
	 *  @param str 			待写入的String对象
	 *  @param dir 			目标文件所在的文件夹
	 *  @param filename 	目标文件的文件名
	 */
	public static void writeStrToFile(String str, String dir, String filename){
		
		String filePathOut = dir + filename;
		File fileOut = new File(filePathOut);
		try{
			FileWriter fWriter = new FileWriter(fileOut);
			BufferedWriter bWriter = new BufferedWriter(fWriter);
			bWriter.write(str);
			bWriter.newLine();
			bWriter.flush();
			bWriter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 把一个HashMap<String, Long>类型的map逐行添加到dir目录下指定文件的末尾
	 * @param map 			待写入的map
	 * @param dir 			目标文件所在的文件夹
	 * @param filename 		目标文件的文件名
	 */
	public static void appendMapToFile(HashMap<String, Long> map, String dir, String filename){
		
		if(map.isEmpty()){
			System.err.println("Specified a null map.");
			return;
		}else{
			//以追加写的方式写入到文件
			String filePathOut = dir + filename;
			File fileOut = new File(filePathOut);
			try{
				FileWriter fWriter = new FileWriter(fileOut, true);
				fWriter.write("\n===============" + new Date() + "===============");
				fWriter.flush();
				
				Set<String> keySet = map.keySet();
				for(String key: keySet){
					String str_temp = "\n" + key + "\t\t" + map.get(key);
					fWriter.write(str_temp);
					fWriter.flush();
				}
				fWriter.close();

			}catch(IOException e){
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 把一个HashMap<String, HashMap<String, Long>>类型的map逐行添加到dir下指定文件的末尾
	 * @param map 			待写入的map
	 * @param dir 			目标文件所在的文件夹
	 * @param filename 		目标文件的文件名
	 */
	public static void appendMapToFile2(HashMap<String, HashMap<String, Long>> map, String dir, String filename){
		
		if(map.isEmpty()){
			System.err.println("Specified a null map.");
			return;
		}else{
			String filePathOut = dir + filename;
			File fileOut = new File(filePathOut);
			try{
				FileWriter fWriter = new FileWriter(fileOut, true);
				fWriter.write("\n===============" + new Date() + "===============");
				fWriter.flush();
				
				Set<String> keySet = map.keySet();
				for(String key: keySet){
					fWriter.write("\n" + key);
					fWriter.flush();
					
					HashMap<String, Long> subMap = map.get(key);
					Set<String> subSet = subMap.keySet();
					for(String subKey: subSet){
						fWriter.write("\n\t" + subKey + "\t\t" + subMap.get(subKey));
						fWriter.flush();
					}
					
				}
				
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	/**
	 *  把一个String以追加写的方式写入dir目录下的filename文件中
	 *  @param str 			待写入的String对象
	 *  @param dir 			目标文件所在的文件夹
	 *  @param filename 	目标文件的文件名
	 */
	public static void appendStrToFile(String str, String dir, String filename){
		
		String filePathOut = dir + filename;
		File fileOut = new File(filePathOut);
		try{
			FileWriter fWriter = new FileWriter(fileOut, true);
			BufferedWriter bWriter = new BufferedWriter(fWriter);
			bWriter.write(str);
			bWriter.newLine();
			bWriter.flush();
			bWriter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	// ************************************************** file readers ********************************************
	// ************************************************************************************************************
	
	/**
	 *  从dir下指定文件filename中读取num行内容，并返回一个String类型的链表
	 *  
	 *  @param lineNum 				待读取的行数
	 *  @param dir 					目标文件所在的文件夹
	 *  @param filename 			目标文件的文件名
	 *  @return LinkedList<String> 	返回读取的内容
	 */
	public static LinkedList<String> getLinesFromFile(int lineNum, String dir, String filename){
		
		LinkedList<String> line_list = new LinkedList<String>();		// 该链表用于存放从文件中读取的内容
		
		String filePathIn = dir + filename;
		File fileIn = new File(filePathIn);
		try{
			FileReader fReader = new FileReader(fileIn);
			BufferedReader bReader = new BufferedReader(fReader);
			
			int count = 0;
			String str_line = "";
			while(bReader.ready() && (count<lineNum)){
				str_line = bReader.readLine();
				if(str_line == null){									// 跳过空行
					System.err.println("Error Read. Null line.");
				}else{
					line_list.add(str_line);
					count++;
				}
			}
			bReader.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return line_list;
	}
	
	/**
	 *  从指定文件filename中删除lineNum行内容
	 * 
	 * @param lineNum 	待删除的行数
	 * @param dir 		目标文件所在的文件夹
	 * @param filename 	目标文件的文件名
	 */
	public static void deleteLinesFromFile(int lineNum, String dir, String filename){
		
		File fileIn = new File(dir + filename);
		try{
			if(!fileIn.isFile()){
				System.err.println(filename + " is not a file. Please check it again.");
				return;
			}else{
				File tempFile = new File(fileIn.getAbsolutePath() + ".tmp");
				BufferedReader bReader = new BufferedReader(new FileReader(fileIn));
				BufferedWriter bWriter = new BufferedWriter(new FileWriter(tempFile));
				
				String str_line = "";
				int count = 0;
				while(bReader.ready()){
					str_line = bReader.readLine();
					count++;
					if(count > lineNum){
						bWriter.write(str_line);
						bWriter.newLine();
						bWriter.flush();
					}
				}
				bReader.close();	//关闭输入流
				bWriter.close();	//关闭输出流
				
				// Delete the original file: fileIn
				if(!fileIn.delete()){
					System.err.println("Could not delete the file");
			        return;
				}
				// Rename the tempFile to fileIn's name: filename
				if(!tempFile.renameTo(fileIn)){
					System.err.println("Could not rename the file");
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 从指定文件中删除指定内容的行
	 * @param list 		待删除的内容
	 * @param dir 		目标文件所在的文件夹
	 * @param filename	目标文件的文件名
	 */
	public static void deleteLinesFromFile(LinkedList<String> list, String dir, String filename){
		
		LinkedList<String> file_list = new LinkedList<String>();
		
		// 从指定文件中读取所有的行并构建一个链表：file_list
		File fileIn = new File(dir + filename);
		if(!fileIn.exists()){
			System.err.println("File not exist. Please check again.");
			return;
		}else{
			try{
				BufferedReader bReader = new BufferedReader(new FileReader(fileIn));
				String str_line = "";
				while(bReader.ready()){
					str_line = bReader.readLine();
					if(str_line == null){
						System.err.println("Error Read. Null line.");
					}else{
						file_list.add(str_line);
					}
				}
				bReader.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		// 从file_list中删除list中包含的内容
		for(String str: list){
			if(file_list.contains(str)){
				file_list.remove(str);
			}
		}
		// 把file_list重新写入文件中，并覆盖文件中的原有内容
		writeListToFile(file_list, dir, filename);
		
	}
	
	/**
	 *  从指定文件filename中读取lineNum行内容，并从文件中删除已读取的行
	 *  将已读取的若干行内容以String链表的形式返回，String链表的每一个元素表示读取的一行内容
	 *  
	 *  @param lineNum 				待读取的行数
	 *  @param dir 					目标文件所在的文件夹
	 *  @param filename 			目标文件的文件名
	 *  @return LinkedList<String> 	返回读取的内容
	 */
	public static LinkedList<String> removeLinesFromFile(int lineNum, String dir, String filename){
		
		LinkedList<String> line_list = new LinkedList<String>();	// 该链表用于存放从文件中读取的内容
		
		File fileIn = new File(dir + filename);
		if(!fileIn.isFile()){
			System.err.println(filename + " is not a file. Please check it again.");
			return null;
			
		}else{
			File tempFile = new File(fileIn.getAbsolutePath() + ".tmp");
			try{
				BufferedReader bReader = new BufferedReader(new FileReader(fileIn));
				BufferedWriter bWriter = new BufferedWriter(new FileWriter(tempFile));
				
				int count = 0;
				String str_line = "";						//从文件中读取的每一行内容
				
				while(bReader.ready()){
					str_line = bReader.readLine();
					count ++;
					if(str_line == null){
						System.err.println("Error Read. Null line.");
					}else if(count <= lineNum){
						line_list.add(str_line);			//把读取的行内容放到line_list的对应位置
					}else{
						bWriter.write(str_line);			//把读取的行内容写入到tempFile中
						bWriter.newLine();
						bWriter.flush();
					}
				}
				bReader.close();
				bWriter.close();
				
				// Delete the original file: fileIn
				if(!fileIn.delete()){
					System.err.println("Could not delete the file");
			        return null;
				}
				// Rename the tempFile to fileIn's name: filename
				if(!tempFile.renameTo(fileIn)){
					System.err.println("Could not rename the file");
				}
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		return line_list;
	}
	
}
