package test;

import java.util.Date;

import util.Constants;

import netease.tools.ArgsNetease;
import netease.tools.ArgsTools;
import netease.tools.LogGenerator;

public class VaringStreamTest {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		int streamInterval = ArgsNetease.streamInterval;
		long clock = System.currentTimeMillis();
		boolean flag = true;
		long count = 0L;
		int controller = 1;
		
		while(true){
			
			controller ++;
			
			if(controller % 2 == 0)
				streamInterval = ArgsNetease.streamInterval;
			else
				streamInterval = 0;
			
			
			flag = true;
			count = 0L;
			clock = System.currentTimeMillis();
			
			while(flag){
				
				String str_line = LogGenerator.recordGenerator();
				System.out.println(str_line);
				count ++;
				
				if((System.currentTimeMillis() - clock) >= 5*1000){
					
					System.out.println("StreamInterval = " + streamInterval + 
							"\nGenerated " + count + " tuples in " + (System.currentTimeMillis() - clock) + " ms.");
					
					flag = false;
				}
				Thread.sleep(streamInterval);
			}	
		}

	}

}
