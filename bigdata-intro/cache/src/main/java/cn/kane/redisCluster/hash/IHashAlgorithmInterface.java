package cn.kane.redisCluster.hash;

public interface IHashAlgorithmInterface {

	/**
	 * 获取指定对象hash值
	 * @param obj
	 * @return
	 */
	Integer hash(Object obj);
	
}
