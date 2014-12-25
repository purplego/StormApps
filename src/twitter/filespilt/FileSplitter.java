package twitter.filespilt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class FileSplitter {

	/**
	 * Twitter数据原始文件太大，无法用工具打开，只好读取一部分数据来观察了
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println("Start to get lines from file ......");
		long starttime = System.currentTimeMillis();
		getLinesFromFile(1000, "D:/Storm/0-Data/1-rawdata/2-Twitter/Data/", "statuses.log.2013-03-31-23", "status.log.part");
		System.out.println("Finish to get lines from file: " + (System.currentTimeMillis() - starttime) + "ms.");
	}

	
	/**
	 *  从dir下指定文件filename中读取num行内容，并将读取的内容写入一个新的文件
	 *  
	 *  @param lineNum 				待读取的行数
	 *  @param dir 					目标文件所在的文件夹
	 *  @param infilename 			待读取的目标文件的文件名
	 *  @param outfilename 			待写入的目标文件的文件名
	 */
	public static void getLinesFromFile(int lineNum, String dir, String infilename, String outfilename){
		
		String filePathIn = dir + infilename;
		String filePathOut = dir + outfilename;
		File fileIn = new File(filePathIn);
		File fileOut = new File(filePathOut);
		try{
			FileReader fReader = new FileReader(fileIn);
			FileWriter fWriter = new FileWriter(fileOut);
			BufferedReader bReader = new BufferedReader(fReader);
			BufferedWriter bWriter = new BufferedWriter(fWriter);
			
			int count = 0;
			String str_line = "";
			while(bReader.ready() && (count<lineNum)){
				str_line = bReader.readLine();
				if(str_line == null){									// 跳过空行
					System.err.println("Error Read. Null line.");
				}else{
					bWriter.write(str_line);
					bWriter.newLine();
					bWriter.flush();
					count++;
				}
			}
			bReader.close();
			bWriter.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
