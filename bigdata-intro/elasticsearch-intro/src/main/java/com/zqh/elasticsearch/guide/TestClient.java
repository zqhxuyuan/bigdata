package com.zqh.elasticsearch.guide;

import junit.framework.TestCase;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.Before;

/**
 * Created by hadoop on 15-1-6.
 *
 */
public class TestClient extends TestCase {

    Client client;

    final String INDEX_NAME = "twitter";
    final String TYPE_NAME = "tweet";

    @Before
    public void setUp() throws Exception {

    }
    @After
    public void tearDown() throws Exception {
        client.close();
    }

    public void testTransportClient(){
        client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    }

    public void testNode(){

    }
}
