package com.bulk;

/**
 * JMXBulkloader for data restore to a single or multi-node Cassandra cluster using the JMX port
 * Created by mikec on 4/2/14
 *
 * Should be executed on each data node using the snapshot folder renamed as /keyspace_name/columnfamily_name/
 * Can be run without modification to the Cassandra yaml file for the running Cassandra instance
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.net.HostAndPort;
import org.apache.cassandra.service.StorageServiceMBean;

public class JmxCassandraBulkLoader {

    private JMXConnector connector;
    private StorageServiceMBean storageBean;

    public JmxCassandraBulkLoader(String host, int port) throws Exception
    {
        connect(host, port);
    }

    private void connect(String host, int port) throws IOException, MalformedObjectNameException
    {
        JMXServiceURL jmxUrl
                = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", host, port));
        Map<String,Object> env = new HashMap<String,Object>();

        connector = JMXConnectorFactory.connect(jmxUrl, env);
        MBeanServerConnection mbeanServerConn = connector.getMBeanServerConnection();
        ObjectName name = new ObjectName("org.apache.cassandra.db:type=StorageService");

        storageBean = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
    }

    /*
     *  Valid format for restore inputs (separated by spaces):
     *  dataPath to sstable snapshot files (/keyspace_name/columnfamily_name/)
     *  hostname:port
     */
    public static void main(String[] args) throws Exception {

        String dataPath;
        String host;
        int port;

        if (args.length != 2) {
            throw new IllegalArgumentException("Invalid arguments. Required: dataPath host:port");
        }

        dataPath = args[0];

        HostAndPort hp = HostAndPort.fromString(args[1])
                .withDefaultPort(7199);
        host = hp.getHostText();
        port = hp.getPort();

        JmxCassandraBulkLoader np = new JmxCassandraBulkLoader(host, port);
        np.bulkLoad(dataPath);

        np.close();
        System.out.print("Done");
        System.out.println();
    }

    public void close() throws IOException
    {
        connector.close();
    }

    public void bulkLoad(String path) {
        storageBean.bulkLoad(path);
    }

}
