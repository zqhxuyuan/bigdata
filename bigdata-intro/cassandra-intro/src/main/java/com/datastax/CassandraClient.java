package com.datastax;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * cassandra 操作类<br>
 * (封装了cassandra client api,它实现了缓冲池)
 * 
 * @author du 2014年7月7日 下午3:50:17
 */
public class CassandraClient {

    private Cluster          cluster;
    private Session          session;
    private static Logger    log                        = LoggerFactory.getLogger(CassandraClient.class);

    private String           keyspace;
    private String[]         agentHosts;
    private int              maxconnectionsPerHost      = 20;
    private int              coreconnectionsPerHost     = 2;
    private int              fetchSize                  = 1000;
    private int              port                       = 9042;
    public static final int  EXECUTE_TIME_FROM_DATABASE = 500;                                        // 数据库执行命令
                                                                                                          // 超时记录时间
    private String           localDc;                                                                    //
    private int              usedHostsPerRemoteDc;                                                       //
    private ConsistencyLevel consistencyLevel           = ConsistencyLevel.SERIAL;

    public void setConsistency(String consistency) {
        try {
            consistencyLevel = Enum.valueOf(ConsistencyLevel.class, consistency);
        } catch (Exception e) {
            log.warn("ConsistencyLevel setting is error, avaliable is : ONE, TWO, THREE, ANY, ALL, ANY, EACH_QUORUM, QUORUM, LOCAL_QUORUM, LOCAL_ONE\n, will use defaule value(SERIAL)");
        }
    }

    public String getLocalDc() {
        return localDc;
    }

    public void setLocalDc(String localDc) {
        this.localDc = localDc;
    }

    public int getUsedHostsPerRemoteDc() {
        return usedHostsPerRemoteDc;
    }

    public void setUsedHostsPerRemoteDc(int usedHostsPerRemoteDc) {
        this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
    }

    public void setAgentHostList(String agentHostList) {
        try {
            this.agentHosts = agentHostList.split(",");
        } catch (Exception e) {
            throw new RuntimeException("error occured when set agent hosts ", e);
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String[] getAgentHosts() {
        return agentHosts;
    }

    public void setAgentHosts(String[] agentHosts) {
        this.agentHosts = agentHosts;
    }

    public int getMaxconnectionsPerHost() {
        return maxconnectionsPerHost;
    }

    public void setMaxconnectionsPerHost(int maxconnectionsPerHost) {
        this.maxconnectionsPerHost = maxconnectionsPerHost;
    }

    public int getCoreconnectionsPerHost() {
        return coreconnectionsPerHost;
    }

    public void setCoreconnectionsPerHost(int coreconnectionsPerHost) {
        this.coreconnectionsPerHost = coreconnectionsPerHost;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * 加载配置参数
     */
    private void showInfo() {
        if (StringUtils.isBlank(keyspace)) {
            log.warn("keyspace not set");
        } else {
            log.info("keyspace is {}", keyspace);
        }

        if (agentHosts == null) {
            throw new RuntimeException("agent hosts not set");
        } else {
            StringBuilder hostText = new StringBuilder();
            for (String host : agentHosts) {
                hostText.append(host).append(", ");
            }

            log.info("agent hosts is {}", hostText.toString());
        }

        log.info("connect port is {}", port);
        log.info("maxconnectionsPerHost is {}", maxconnectionsPerHost);
        log.info("coreconnectionsPerHost is {}", coreconnectionsPerHost);
        log.info("fetchSize is {}", fetchSize);
        log.info("local data center is {}", localDc);
        log.info("the number of host per remote datacenter is {}", usedHostsPerRemoteDc);
    }

    public void init() {
        showInfo();

        Builder builder = Cluster.builder();

        // 连接池配置
        PoolingOptions poolingOptions = new PoolingOptions().setMaxConnectionsPerHost(HostDistance.REMOTE,
                                                                                      this.maxconnectionsPerHost).setCoreConnectionsPerHost(HostDistance.REMOTE,
                                                                                                                                            this.coreconnectionsPerHost);
        builder.withPoolingOptions(poolingOptions);

        // socket 链接配置
        SocketOptions socketOptions = new SocketOptions().setKeepAlive(true).setReceiveBufferSize(1024 * 1024).setSendBufferSize(1024 * 1024).setConnectTimeoutMillis(5 * 1000).setReadTimeoutMillis(1000);
        builder.withSocketOptions(socketOptions);

        // Query 配置
        // QueryOptions queryOptions = new
        // QueryOptions().setFetchSize(this.fetchSize).setSerialConsistencyLevel(ConsistencyLevel.ONE).setConsistencyLevel(ConsistencyLevel.ONE);
        QueryOptions queryOptions = new QueryOptions().setFetchSize(this.fetchSize).setConsistencyLevel(consistencyLevel);
        builder.withQueryOptions(queryOptions);

        // 重试策略
        // builder.withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);

        // 加载平衡策略
        DCAwareRoundRobinPolicy dCAwareRoundRobinPolicy = new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc);
        builder.withLoadBalancingPolicy(dCAwareRoundRobinPolicy);

        // 压缩
        builder.withCompression(Compression.LZ4);

        // 添加节点
        for (String host : this.agentHosts) {
            builder.addContactPoints(host);
        }

        builder.withPort(this.port);

        cluster = builder.build();
        session = cluster.connect(keyspace);
        log.info("Connected to cluster: {}\n", cluster.getMetadata().getClusterName());

    }

    /**
     * 关闭client<br>
     */
    public void close() {
        if (null != session) {
            session.close();
        }

        if (null != cluster) {
            cluster.close();
        }

        cluster = null;

        log.info("client destroied");
    }

    /**
     * 执行sql,没有结果返回,并关闭自动创建的session
     * 
     * @Throws: NoHostAvailableException - if no host in the cluster can be contacted successfully to execute this
     * query. QueryExecutionException - if the query triggered an execution exception, i.e. an exception thrown by
     * Cassandra when it cannot execute the query with the requested consistency level successfully.
     * QueryValidationException - if the query if invalid (syntax error, unauthorized or any other validation problem).
     * @param cql
     */
    public void executeWithoutResult(String cql) {
        log.debug("execute cassandra cql: {}", cql);
        session.execute(cql);
    }

    /**
     * 执行sql,没有结果返回,并关闭自动创建的session
     * 
     * @Throws: NoHostAvailableException - if no host in the cluster can be contacted successfully to execute this
     * query. QueryExecutionException - if the query triggered an execution exception, i.e. an exception thrown by
     * Cassandra when it cannot execute the query with the requested consistency level successfully.
     * QueryValidationException - if the query if invalid (syntax error, unauthorized or any other validation problem).
     * @param cql
     */
    public ResultSet execute(String cql) {
        log.debug("execute cassandra cql: {}", cql);
        return session.execute(cql);
    }

    /**
     * 返回一个初始化好的session<br>
     * 切忌不要调用close
     * 
     * @return
     */
    public Session getSession() {
        return session;
    }

    /**
     * 根据keys查找所有的数据
     * 
     * @param cql
     * @param paramValues
     * @return 一行数据或者null
     */
    public Row getOne(String cql, Object... paramValues) {
        if (checkParamsExsitsBlankValue(paramValues)) {
            return null;
        }

        ResultSet rs = null;

        try {
            log.debug("execute cql:{} on {} keyspace ", cql, keyspace);
            StringBuilder logText = new StringBuilder("prepare params : ");
            for (int i = 0; paramValues != null && i < paramValues.length; i++) {
                logText.append("{}, ");
            }

            log.debug(logText.toString(), paramValues);
            rs = session.execute(cql, paramValues);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return null;
        }

        return rs.one();
    }

    /**
     * @param paramValues
     * @return 如果 paramValues不为空，且存在空值(null或者""或者类似"  ")，则返回true， 否则false
     */
    private boolean checkParamsExsitsBlankValue(Object... paramValues) {
        if (null == paramValues) {
            return false;
        }

        for (Object o : paramValues) {
            if (o == null || StringUtils.isBlank(o.toString())) {
                return true;
            }
        }

        return false;
    }

    public ResultSet execute(Statement stmt) {
        return session.execute(stmt);
    }

    public Row getOne(BoundStatement bstmt) {
        try {
            return session.execute(bstmt).one();
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return null;
        }
    }

    public Row getOne(PreparedStatement pstmt, Object... paramValues) {
        if (checkParamsExsitsBlankValue(paramValues)) {
            return null;
        }

        try {
            BoundStatement bstmt = pstmt.bind(paramValues);
            return getOne(bstmt);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return null;
        }
    }

    public List<Row> getAll(BoundStatement bstmt) {

        ResultSet rs = null;
        try {
            rs = session.execute(bstmt);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return Collections.emptyList();
        }

        return rs.all();
    }

    /**
     * 获取所有，
     * 
     * @param bstmt
     * @param size 限制大小
     * @return 数据列表， 数据小于等于size大小
     */
    public List<Row> getAllOfSize(BoundStatement bstmt, int size) {
        List<Row> resultRows = new ArrayList<>();
        int count = 0;
        ResultSet rs = null;
        try {
            rs = session.execute(bstmt);
            Iterator<Row> it = rs.iterator();
            while (it.hasNext() && count < size) {
                count++;
                resultRows.add(it.next());
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return Collections.emptyList();
        }

        return resultRows;
    }

    public List<Row> getAll(PreparedStatement pstmt, Object... paramValues) {
        if (checkParamsExsitsBlankValue(paramValues)) {
            return Collections.emptyList();
        }

        try {
            BoundStatement bstmt = pstmt.bind(paramValues);
            return getAll(bstmt);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * @param pstmt
     * @param count
     * @param paramValues
     * @return
     */
    public List<Row> getAllOfSize(PreparedStatement pstmt, int count, Object... paramValues) {
        if (checkParamsExsitsBlankValue(paramValues)) {
            return Collections.emptyList();
        }

        try {
            BoundStatement bstmt = pstmt.bind(paramValues);
            return getAllOfSize(bstmt, count);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    public void execute(PreparedStatement pstmt, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        session.execute(bstmt);

    }
}
