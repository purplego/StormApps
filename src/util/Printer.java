package util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

public class Printer {
	
	/**
	 * 在控制台中逐个打印String类型数组str_array
	 * @param str_array
	 */
	public static void printArray(String[] str_array){
		System.out.println("************ beigin to print the String array ************");
		for(String str: str_array){
			System.out.println(str);
		}
		System.out.println();
	}
	
	/**
	 * 在控制台中逐个打印String类型的链表list
	 * @param list
	 */
	public static void printList(LinkedList<String> list){
		System.out.println("************ beigin to print the String list ************");
		for(String str: list){
			System.out.println(str);
		}
		System.out.println();
	}
	
	/**
	 * 在控制台中逐个打印String类型的HashMap
	 * @param map
	 */
	public static void printMap(HashMap<String, Long> map){
		System.out.println("************ beigin to print the String Map ************");
		Set<String> key_set = map.keySet();
		for(String str: key_set){
			System.out.println(str + "\t\t\t" + map.get(str));
		}
		System.out.println();
	}
	
	public static void printMap2(HashMap<String, HashMap<String, Long>> map){
		System.out.println("************ beigin to print the HashMap<String, HashMap<String, Long>> Map ************");
		Set<String> key_set = map.keySet();
		for(String str: key_set){
			System.out.println(str);
			HashMap<String, Long> tmp_map = map.get(str);
			Printer.printMap(tmp_map);
		}
		System.out.println();
	}
}
