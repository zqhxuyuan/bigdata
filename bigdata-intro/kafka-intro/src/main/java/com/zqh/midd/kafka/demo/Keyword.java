package com.zqh.midd.kafka.demo;

import java.util.Date;

/**
 * http://my.oschina.net/ielts0909/blog/100645
 *
 * 应用场景: 
 * 一个站内的搜索引擎，运营人员想知道某一时段，各类用户对商品的不同需求。
 * 通过对这些数据的分析，从而获得更多有价值的市场分析报表。
 * 这样的情况，就需要我们对每次的搜索进行记录，当然，不太可能使用数据库区记录这些信息。
 * 最好的办法是存日志。然后通过对日志的分析，计算出有用的数据。我们采用kafka这种分布式日志系统来实现这一过程。
 * 
 * 完成上述一系列的工作，可以按照以下步骤来执行：
 * 1. 搭建kafka系统运行环境。
 * 2. 设计数据存储格式（按照自定义格式来封装消息）
 * 3. Producer端获取真实数据（搜索记录），并对数据按上述2中设计的格式进行编码。
 * 4. Producer将已经编码的数据发送到broker上，在broker上进行存储（分配存储策略）。
 * 5. Consumer端从broker中获取数据，分析计算。
 */
public class Keyword {

	private Integer id;		// 消息的ID
	private String user;	// 用户
	private String keyword;	// 查询关键词
	private Date date;		// 查询时间
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	
	@Override
	public String toString() {
		return "MyMessage [id=" + id + ", user=" + user + ", keyword="
				+ keyword + ", date=" + date + "]";
	}
	
}
