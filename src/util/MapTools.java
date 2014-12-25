package util;

import java.util.HashMap;
import java.util.Set;

public class MapTools {
	
	/**
	 * 按照一定格式，将一个HashMap<String, Long>类型的map转换为一个字符串
	 * 例如map中有两个node，2013-1-15:10 14和2013-1-15:11 20.则按如下格式转换
	 * 2013-1-15:10@14/2013-1-15:11@20
	 * 
	 * @param map 待转换的HashMap<String, Long>
	 * @return 转换后的字符串
	 */
	public static String mapToString(HashMap<String, Long> map){
		
		String mapStr = "";
		if(map.isEmpty()){
			System.err.println("In the mapToString method, You specified an EMPTY map.Check again.");
		}else{
			
			Set<String> keySet = map.keySet();
			for(String key: keySet){
				mapStr += key + "@" + map.get(key) + "/";
			}
			mapStr = mapStr.substring(0, mapStr.lastIndexOf("/"));
		}
		
		return mapStr;
	}
	
	/**
	 * 按照一定格式，将一个HashMap<String, HashMap<String, Long>>类型的map转换为一个字符串
	 * 例如map中有两个node，2013-1-15（10.107.11.15=10 10.107.11.16=20 10.107.11.17=30），
	 * 以及2013-1-16（10.107.11.15=25 10.107.11.16=35 10.107.11.17=45），则按如下格式转换
	 * 2013-1-15@10.107.11.15=10%10.107.11.16=20%10.107.11.17=30/2013-1-16@10.107.11.15=25%10.107.11.16=35%10.107.11.17=45
	 * 
	 * @param map 待转换的HashMap<String, HashMap<String, Long>>
	 * @return 转换后的字符串
	 */
	public static String mapToString2(HashMap<String, HashMap<String, Long>> map){
		String mapStr = "";
		
		if(map.isEmpty()){
			System.err.println("In the mapToString2 method, You specified an EMPTY map. Check again.");
		}else{
			
			Set<String> keySet = map.keySet();
			for(String key: keySet){
				String subMapStr = "";
				
				HashMap<String, Long> subMap = map.get(key);
				
				if(subMap.isEmpty()){
					System.err.println("In the mapToString2 method, You got an EMPTY sub-map.");
				}else{
					
					Set<String> subKeySet = subMap.keySet();
					for(String subKey: subKeySet){
						subMapStr += subKey + "=" + subMap.get(subKey) + "%";
					}
					subMapStr = subMapStr.substring(0, subMapStr.lastIndexOf("%"));
					
					mapStr += key + "@" + subMapStr + "/";
				}
				
			}
			mapStr = mapStr.substring(0, mapStr.lastIndexOf("/"));
		}
		
		return mapStr;
	}
	/**
	 * mapToString的逆方法，依照mapToString中的规则把string转换成对应类型的map，待转换的字符串形如
	 * 2013-1-15:10@14/2013-1-15:11@20
	 * 
	 * @param mapStr  待转换的字符串
	 * @return	转换后的HashMap<String, Long>
	 */
	public static HashMap<String, Long> strToMap(String mapStr){
		HashMap<String, Long> map = new HashMap<String, Long>();
		if(mapStr != null){
			String[] node_array = mapStr.split("/");
			for(String node: node_array){
				String[] array = node.split("@");
				map.put(array[0], Long.parseLong(array[1]));
			}
		}else{
			// Do nothing
		}
		
		return map;
	}
	
	/**
	 * mapToString2的逆方法,依照mapToString2中的规则把string转换成对应类型的map，待转换的字符串形如
	 * 2013-1-15@10.107.11.15=10%10.107.11.16=20%10.107.11.17=30/2013-1-16@10.107.11.15=25%10.107.11.16=35%10.107.11.17=45
	 * 
	 * @param mapStr  待转换的字符串
	 * @return	转换后的HashMap<String, HashMap<String, Long>>
	 */
	public static HashMap<String, HashMap<String, Long>> strToMap2(String mapStr){
		
		HashMap<String, HashMap<String, Long>> map = new HashMap<String, HashMap<String, Long>>();
		if(mapStr != null){
			
			String[] node_array = mapStr.split("/");
			for(String node: node_array){
				
				String[] subNode_array = node.split("@");
				String key = subNode_array[0];
				String value = subNode_array[1];
				
				HashMap<String, Long> subMap = new HashMap<String, Long>();
				String[] value_array = value.split("%");
				for(String item: value_array){
					String[] array = item.split("=");
					subMap.put(array[0], Long.parseLong(array[1]));
				}
				map.put(key, subMap);
			}
		}else{
			// Do nothing
		}
		
		return map;
	}
	
	/**
	 * 把两个HashMap<String, Long>类型的map，即 map和map_tmp相加，并返回map
	 * @param map
	 * @param map_tmp
	 * @return
	 */
	public static HashMap<String, Long> addMap(HashMap<String, Long> map, HashMap<String, Long> map_tmp){
		
		if((!map.isEmpty()) && (!map_tmp.isEmpty())){
			
			Set<String> keySet = map_tmp.keySet();
			for(String key: keySet){
				if(map.containsKey(key)){
					long value = map.get(key);
					value += map_tmp.get(key);
					map.put(key, value);
					
				}else{
					map.put(key, map_tmp.get(key));
				}
			}
			
		}else if((!map.isEmpty()) && (map_tmp.isEmpty())){
			return map;
			
		}else if((map.isEmpty()) && (!map_tmp.isEmpty())){
			return map_tmp;
			
		}else if((map.isEmpty()) && (map_tmp.isEmpty())){
			return null;
		}
		
		return map;
	}
	
	/**
	 * 把两个HashMap<String, HashMap<String, Long>> 类型的map，即map和map_tmp相加，并返回map
	 * @param map
	 * @param map_tmp
	 * @return
	 */
	public static HashMap<String, HashMap<String, Long>> addMap2(
			HashMap<String, HashMap<String, Long>> map, HashMap<String, HashMap<String, Long>> map_tmp){
		
		if((!map.isEmpty()) && (!map_tmp.isEmpty())){
			
			Set<String> keySet = map_tmp.keySet();
			for(String key: keySet){
				if(map.containsKey(key)){
					
					HashMap<String, Long> valueMap1 = map.get(key);
					HashMap<String, Long> valueMap2 = map_tmp.get(key);
					
					valueMap1 = MapTools.addMap(valueMap1, valueMap2);
					map.put(key, valueMap1);
				}else{
					map.put(key, map_tmp.get(key));
				}
			}
			
		}else if((!map.isEmpty()) && (map_tmp.isEmpty())){
			return map;
			
		}else if((map.isEmpty()) && (!map_tmp.isEmpty())){
			return map_tmp;
			
		}else if((map.isEmpty()) && (map_tmp.isEmpty())){
			return null;
		}
		
		return map;
	}
	
	/**
	 * 把一个键值对加到已有的map中去，并返回map
	 * @param map		已有的HashMap<String, Long>map
	 * @param key		待添加元素的key值
	 * @param value		待添加元素的value
	 * @return			更新后的map
	 */
	public static HashMap<String, Long> addElementToMap(HashMap<String, Long> map, String key, Long value){
		
		if(!map.isEmpty()){
			
			if(map.containsKey(key)){
				value = map.get(key) + value;
			}
		}
		map.put(key, value);
		
		return map;
	}
	
	/**
	 * 把一个键值对加到已有的map中去，并返回map
	 * @param map		已有的HashMap<String, HashMap<String, Long>>map
	 * @param key		待添加元素的key值(外层map的key)
	 * @param subKey	待添加元素的key值（内层map的key）
	 * @param value		待添加元素的value
	 * @return			更新后的map
	 */
	public static HashMap<String, HashMap<String, Long>> addElementToMap2(HashMap<String, HashMap<String, Long>> map, 
			String key, String subKey, Long value){
		
		HashMap<String, Long> subMap = new HashMap<String, Long>();
		if(!map.isEmpty()){
			if(map.containsKey(key)){
				
				subMap = map.get(key);
				if(subMap.containsKey(subKey)){
					value = subMap.get(subKey) + value;
				}
			}
		}
		subMap.put(subKey, value);
		map.put(key, subMap);
		
		return map;
	}
}
