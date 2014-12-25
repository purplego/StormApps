package netease.tools;

import java.util.LinkedList;

public class SpiderSite {
	
	private LinkedList<String> spider_list; //该变量中列出了常见的10种搜索引擎蜘蛛
	
	
	/**
	 * 初始时，该list中列出了10中常见的搜索引擎蜘蛛
	 * 返回值为经过初始化的搜索引擎蜘蛛链表
	 */
	public SpiderSite(){
		
		LinkedList<String> spider_list = new LinkedList<String>();
		spider_list.add("googlebot");
		spider_list.add("baiduspider");
		spider_list.add("baidugame");
		spider_list.add("msnbot");
		spider_list.add("bingbot");
		spider_list.add("ahrefsbot");
		spider_list.add("360spider");
		spider_list.add("nutch");
		spider_list.add("sosospider");
		spider_list.add("sogou web spider");
		
		this.spider_list = spider_list;
	}
	
	/**
	 * 向list中添加一个蜘蛛网站
	 * @param spider
	 */
	public void addSpider(String spider){
		if(this.spider_list.contains(spider.toLowerCase())){
			this.spider_list.add(spider.toLowerCase());
		}else{
			return;
		}
	}
	
	/**
	 * 从list中删除一个蜘蛛网站
	 * @param spider
	 */
	public void removeSpider(String spider){
		if(this.spider_list.contains(spider.toLowerCase())){
			this.spider_list.remove(spider.toLowerCase());
		}else{
			return;
		}
	}
	
	/**
	 * 判断一个agent是不是蜘蛛，即判断该agent字段中是否包含了蜘蛛的字段：如果包含了，则返回蜘蛛名字；否则返回null
	 * @param agent mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)
	 * @return 如果agent是spider，则返回spiderName；否则返回null
	 */
	public String containsSpider(String agent){
		String spiderName = "";
		agent = agent.trim().toLowerCase();
		for(String spider: this.spider_list){
			if(agent.contains(spider)){
				spiderName = spider;
				break;
			}else{
				spiderName = null;
			}
		}
		return spiderName;
	}

	/**
	 * 判断一个agent是不是蜘蛛，即判断该agent字段中是否包含了蜘蛛的字段：如果包含了，则返回true；否则返回false
	 * @param agent
	 * @return
	 */
	public boolean contains(String agent){
		agent = agent.trim().toLowerCase();
		for(String spider: this.spider_list){
			if(agent.contains(spider)){
				return true;
			}
		}
		
		return false;
	}
	
	public void setSpiderList(LinkedList<String> spider_list) {
		this.spider_list = spider_list;
	}

	public LinkedList<String> getSpiderList() {
		return spider_list;
	}
}
