package com.zqh.paas.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.zqh.paas.util.CiperTools;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.zqh.paas.PaasException;
import com.zqh.paas.util.StringUtil;

/**
 * 统一配置ZK管理类，实现初始化自动创建
 * ZK作为配置中心, 管理一些中间件的地址,入口,资源/服务的配置参数
 */
public class ConfigurationCenter {
	private static final Logger log = Logger.getLogger(ConfigurationCenter.class);
	private final String UNIX_FILE_SEPARATOR = "/";

	private ZooKeeper zk = null; //zookeeper客户端实例,用于读写zookeeper节点

	private String centerAddr = null; //zookeeper集群的地址
	private boolean createZKNode = false; //是否在启动时由程序通过properties文件创建ZK节点
	private int timeOut = 2000;

	private String runMode = PROD_MODE;// P:product mode; D:dev mode
	public static final String DEV_MODE = "D";
	public static final String PROD_MODE = "P";
    //ZK的配置信息用properties文件来表示,方便在没有存在节点的情况下由程序直接创建,也便于管理
	private List<String> configurationFiles = new ArrayList<String>();
    //对应properties文件的配置信息
	private Properties props = new Properties();

    private String auth = null;

    // 订阅者. key:path, value:AppImpl
    // 比如path=/com/zqh/paas/cache/conf, 其实现类为cacheSv:com.zqh.paas.cache.impl.RedisCache
	private HashMap<String, ArrayList<ConfigurationWatcher>> subsMap = null;

	public ConfigurationCenter(String centerAddr, int timeOut, String runMode, List<String> configurationFiles) {
		this.centerAddr = centerAddr;
		this.timeOut = timeOut;
		this.runMode = runMode;
		if (configurationFiles != null) {
			this.configurationFiles.addAll(configurationFiles);
		}
	}

	public ConfigurationCenter(String centerAddr, int timeOut, String runMode) {
		this.centerAddr = centerAddr;
		this.timeOut = timeOut;
		this.runMode = runMode;
	}

    /**
     * 初始化连接ZooKeeper
     */
    public void init() {
        try {
            for (String configurationFile : configurationFiles) {
                props.load(this.getClass().getResourceAsStream(configurationFile));
            }
        } catch (IOException e) {
            log.error("Error load proerpties file," + configurationFiles, e);
        }
        // 增加watch,开发模式没有，生产有
        try {
            zk = connectZookeeper(centerAddr, timeOut, runMode);
        } catch (Exception e) {
            log.error("Error connect to Zookeeper," + centerAddr, e);
        }
        subsMap = new HashMap<String, ArrayList<ConfigurationWatcher>>();
        if (isCreateZKNode()) {
            writeData();
        }
    }

	private ZooKeeper connectZookeeper(String address, int timeout, String runMode) throws Exception {
		if (DEV_MODE.equals(runMode)) {
			ZooKeeper zk = new ZooKeeper(centerAddr, timeout, new Watcher() {
				public void process(WatchedEvent event) {
					// 不做处理
				}
			});
			return zk;
		} else {
			ZooKeeper zk = new ZooKeeper(centerAddr, timeout, new Watcher() {
                /**
                 * 创建ZooKeeper客户端实例时, 指定通知机制
                 * @param event
                 */
				public void process(WatchedEvent event) {
					if (log.isInfoEnabled()) {
						log.info(event.toString());
					}
					if (Event.EventType.NodeDataChanged.equals(event.getType()) && subsMap.size() > 0) {
						String path = event.getPath();
						ArrayList<ConfigurationWatcher> watcherList = subsMap.get(path);
						if (watcherList != null && watcherList.size() > 0) {
							for (ConfigurationWatcher watcher : watcherList) {
								try {
									watcher.process(getConf(path));
								} catch (PaasException e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			});
            if ((this.auth != null) && (this.auth.length() > 0))
                this.zk.addAuthInfo("digest", CiperTools.decrypt(this.auth).getBytes());
			return zk;
		}
	}

    //----------START:创建节点,并设置值.包括在父节点不存在的情况下,创建父节点.
	@SuppressWarnings("rawtypes")
	private void writeData() {
		// 开始创建节点
		Set keyValue = props.keySet();
		for (Iterator it = keyValue.iterator(); it.hasNext();) {
			String path = (String) it.next();
			String pathValue = (String) props.getProperty(path);
			// 开始创建
			try {
				setZKPathNode(zk, path, pathValue);
			} catch (Exception e) {
				log.error("Error create to set node data,key=" + path + ",value=" + pathValue, e);
			}
		}
	}

	private void setZKPathNode(ZooKeeper zk, String path, String pathValue) throws Exception {
		if (zk.exists(path, false) == null) {
			createPathNode(zk, path.split(UNIX_FILE_SEPARATOR));
		}
		// 设置值,匹配所有版本
		zk.setData(path, pathValue.getBytes("UTF-8"), -1);
		log.info("Set zk node data: node=" + path + ",value=" + pathValue);
	}

	private void createPathNode(ZooKeeper zk, String[] pathParts) throws Exception {
		StringBuilder path = new StringBuilder();
		for (int i = 0; i < pathParts.length; i++) {
			if (!StringUtil.isBlank(pathParts[i])) {
				path.append(UNIX_FILE_SEPARATOR).append(pathParts[i]);
				String pathString = path.toString();
				try {
					if (zk.exists(pathString, false) == null) {
						// 前面都是空
						zk.create(pathString, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				} catch (KeeperException e) {
					if (e.code() != KeeperException.Code.NODEEXISTS)
						throw e;
				}
			}
		}
	}
    //----------END

	@SuppressWarnings("rawtypes")
	public void removeWatcher(String confPath, Class warcherClazz) throws PaasException {
		ArrayList<ConfigurationWatcher> watcherList = subsMap.get(confPath);
		try {
			if (watcherList == null) {
				zk.getData(confPath, false, null);
			} else {
				int size = watcherList.size();
				// ConfigurationWatcher watcher = null;
				for (int i = size - 1; i >= 0; i--) {
					if (watcherList.get(i).getClass().equals(warcherClazz)) {
						watcherList.remove(i);
					}
				}
				if (watcherList.size() == 0) {
					zk.getData(confPath, false, null);
				}
			}
		} catch (Exception e) {
			log.error("", e);
			throw new PaasException("9999", "failed to get configuration from configuration center", e);
		}

	}

	public String getRunMode() {
		return runMode;
	}

	public List<String> getConfigurationFiles() {
		return configurationFiles;
	}

	public void setConfigurationFile(List<String> configurationFile) {
		this.configurationFiles.addAll(configurationFiles);
	}

    /**
     * 订阅者的实现类在初始化后应该注册到ConfCenter
     *
     * 配置中心只提供读取节点数据的能力. 具体的服务类要自己在初始化的时候根据自己的节点路径,调用该方法获取节点数据,
     * 然后从节点数据中构建自己的服务对象: 因为节点数据保存的就是这个服务类的配置信息,可以用于构建服务对象
     * @param confPath
     * @param warcher
     */
    public String getConfAndWatch(String confPath, ConfigurationWatcher warcher) throws PaasException {
        ArrayList<ConfigurationWatcher> watcherList = subsMap.get(confPath);
        if (watcherList == null) {
            watcherList = new ArrayList<ConfigurationWatcher>();
            subsMap.put(confPath, watcherList);
        }
        watcherList.add(warcher);
        return this.getConf(confPath);
    }

    /**
     * 根据zookeeper的节点路径,获取这个节点的配置信息. 通常某个节点是针对某一类服务,所以节点的数据内容就是这个服务的配置信息
     * @param confPath
     * @return
     * @throws PaasException
     */
    public String getConf(String confPath) throws PaasException {
        String conf = null;
        try {
            if (DEV_MODE.equals(this.getRunMode())) {
                return props.getProperty(confPath);
            } else {
                //比较重要的是第二个参数表示有通知机制: 在获取节点数据时(初始化服务类),如果节点数据发生变化,通知客户端
                //通知的实现类在构造ZooKeeper时指定:匿名的Watcher实例
                conf = new String(zk.getData(confPath, true, null),"UTF-8");
            }
        } catch (Exception e) {
            log.error("", e);
            throw new PaasException("9999", "failed to get configuration from configuration center", e);
        }
        return conf;
    }

	public void destory() {
		if (null != zk) {
			try {
				log.info("Start to closing zk client," + zk);
				zk.close();
				log.info("ZK client closed," + zk);
			} catch (InterruptedException e) {
				log.error("Can not close zk client", e);
			}
		}
	}

	public int getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}

	public boolean isCreateZKNode() {
		return createZKNode;
	}

	public void setCreateZKNode(boolean createZKNode) {
		this.createZKNode = createZKNode;
	}

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }
}
