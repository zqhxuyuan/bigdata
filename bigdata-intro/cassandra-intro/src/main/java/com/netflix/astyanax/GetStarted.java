package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Created by zhengqh on 15/9/1.
 *
 * https://github.com/Netflix/astyanax/wiki/Getting-Started
 */
public class GetStarted {

    public static void main(String[] args) throws Exception{
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("Test Cluster")
                .forKeyspace("netflix")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                .setPort(9160)
                                .setMaxConnsPerHost(1)
                                .setSeeds("192.168.6.53:9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getClient();

        ColumnFamily<String, String> CF_USER_INFO =
                new ColumnFamily<String, String>(
                        "Standard1",              // Column Family Name
                        StringSerializer.get(),   // Key Serializer
                        StringSerializer.get());  // Column Serializer

        // Inserting data
        MutationBatch m = keyspace.prepareMutationBatch();

        m.withRow(CF_USER_INFO, "acct1234")
                .putColumn("firstname", "john", null)
                .putColumn("lastname", "smith", null)
                .putColumn("address", "555 Elm St", null)
                .putColumn("age", 30, null);
        //m.withRow(CF_USER_STATS, "acct1234").incrementCounterColumn("loginCount", 1);

        try {
            OperationResult<Void> result = m.execute();
        } catch (ConnectionException e) {
        }
    }
}
