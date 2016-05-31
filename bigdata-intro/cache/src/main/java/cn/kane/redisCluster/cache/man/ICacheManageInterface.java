package cn.kane.redisCluster.cache.man;

public interface ICacheManageInterface {

	/**
	 * re-connect
	 */
	void reConn() ;
	
	/**
	 * ping cacheServer
	 * @return
	 */
	String ping() ;
	
	/**
	 * be slaveOf master
	 * @param masterHost
	 * @param port
	 */
	void slaveOf(String masterHost,int port) ;
	
	/**
	 * be master
	 */
	void beMaster();
	
	/**
	 * cacheServer-info
	 * @return
	 */
	String cacheServerInfo();
	
	
}
