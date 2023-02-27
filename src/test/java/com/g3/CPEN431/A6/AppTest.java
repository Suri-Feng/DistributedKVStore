package com.g3.CPEN431.A6;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.protobuf.ByteString;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Queue;

/**
 * Unit test for simple App.
 */
public class AppTest
        extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        //ClientHandler.run();
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() throws SocketTimeoutException, UnknownHostException, InterruptedException {
        assertTrue( putOne());
    }

    public Boolean putOne() throws SocketTimeoutException, UnknownHostException, InterruptedException {
        String remote_host = "";
        int remote_port = 0;

        ByteString key = ByteString.copyFrom("key".getBytes());
        ByteString val = ByteString.copyFrom("val".getBytes());
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(0x01)
                .setKey(key)
                .setValue(val)
                .setVersion(0)
                .build();

        Queue<KeyValueRequest.KVRequest> requests = null;
        requests.add(request);
        OneClientThread.run(remote_host, remote_port, requests);
        return true;
    }
}
