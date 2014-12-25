package test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

import util.Constants;
import util.FileUtil;

public class RemoteFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		System.out.println("Start to test...");
		LinkedList<File> list = new LinkedList<File>();
		String filePath = "//10.107.18.201/data/";
		File file = new File(filePath);
		if(file.exists()){
			File[] files = file.listFiles();
			System.out.println("There are " + files.length + " files in total.");
			/*for(File f: files){
				System.out.println(f.getName());
				list.add(f);
			}
			
			Collections.sort(list);
			for(File fi: list){
				FileUtil.appendStrToFile(fi.toString(), Constants.RESOURCE_DIRECTORY, "fileList.txt");
			}*/
			
		}else{
			System.err.println("Error with the file. File not exist.");
		}
		
	}

}
