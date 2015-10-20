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

public class CassClient {

    private Cluster          cluster;
    private Session          session;
    private static Logger    log                        = LoggerFactory.getLogger(CassClient.class);

    private String           keyspace;
    private String[]         agentHosts;
    private int              maxconnectionsPerHost      = 20;
    private int              coreconnectionsPerHost     = 2;
    private int              fetchSize                  = 1000;
    private int              port                       = 9042;
    public static final int  EXECUTE_TIME_FROM_DATABASE = 500;     // 数据库执行命令超时记录时间
    private String           localDc;                                                                    
    private int              usedHostsPerRemoteDc;                                                       
    private ConsistencyLevel consistencyLevel           = ConsistencyLevel.SERIAL;

    public void setConsistency(String consistency) {
        try {
            consistencyLevel = Enum.valueOf(ConsistencyLevel.class, consistency);
        } catch (Exception e) {
            log.warn("ConsistencyLevel setting is error, avaliable is : ONE, TWO, THREE, ANY, ALL, ANY, EACH_QUORUM, QUORUM, LOCAL_QUORUM, LOCAL_ONE\n, will use defaule value(SERIAL)");
        }
    }

    public void setLocalDc(String localDc) {
        this.localDc = localDc;
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

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setAgentHosts(String[] agentHosts) {
        this.agentHosts = agentHosts;
    }

    public void setMaxconnectionsPerHost(int maxconnectionsPerHost) {
        this.maxconnectionsPerHost = maxconnectionsPerHost;
    }

    public void setCoreconnectionsPerHost(int coreconnectionsPerHost) {
        this.coreconnectionsPerHost = coreconnectionsPerHost;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void init() {
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
        // QueryOptions queryOptions = new QueryOptions().setFetchSize(this.fetchSize).setSerialConsistencyLevel(ConsistencyLevel.ONE).setConsistencyLevel(ConsistencyLevel.ONE);
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

    public ResultSet execute(String cql) {
        return session.execute(cql);
    }

    public Session getSession() {
        return session;
    }

    /**
     * 根据keys查找所有的数据
     * @return 一行数据或者null
     */
    public Row getOne(String cql, Object... paramValues) {
        ResultSet rs = session.execute(cql, paramValues);
        return rs.one();
    }

    public ResultSet execute(Statement stmt) {
        return session.execute(stmt);
    }

    public Row getOne(BoundStatement bstmt) {
        return session.execute(bstmt).one();
    }

    public Row getOne(PreparedStatement pstmt, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        return getOne(bstmt);
    }

    public List<Row> getAll(BoundStatement bstmt) {
        ResultSet rs = session.execute(bstmt);
        return rs.all();
    }

    public List<Row> getAllOfSize(BoundStatement bstmt, int size) {
        List<Row> resultRows = new ArrayList<>();
        int count = 0;
        ResultSet rs = session.execute(bstmt);
        Iterator<Row> it = rs.iterator();
        while (it.hasNext() && count < size) {
            count++;
            resultRows.add(it.next());
        }
        return resultRows;
    }

    public List<Row> getAll(PreparedStatement pstmt, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        return getAll(bstmt);
    }

    public List<Row> getAllOfSize(PreparedStatement pstmt, int count, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        return getAllOfSize(bstmt, count);
    }

    public void execute(PreparedStatement pstmt, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        session.execute(bstmt);
    }
}
