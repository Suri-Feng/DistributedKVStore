package com.g3.CPEN431.project;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import static com.g3.CPEN431.project.TestHandler.*;


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

    public void testPUTGETREM() throws SocketTimeoutException, UnknownHostException, InterruptedException {
        //assertTrue( putOneGetOne());
        UDPClient client = new UDPClient();
        client.createSocket();

//        wipeOut(client);
//        Thread.sleep(1000);

        // PUT->PUT->REM
        OutcomePair outcome0 = get("K1", client);
        printOutcome(outcome0); // EXPECT GET(K1) -> [TIMEOUT, NULL], [K-N-F, NULL]

        OutcomePair outcome1 = put("K1", "1", 0, client);
        printOutcome(outcome1); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]

        OutcomePair outcome2 = get("K1", client);
        printOutcome(outcome2); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1]

        OutcomePair outcome3 = put("K1", "2", 1, client);
        printOutcome(outcome3); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]

        OutcomePair outcome4 = get("K1", client);
        printOutcome(outcome4); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1], [SUCCESS, 3]

        OutcomePair outcome5 = remove("K1", client);
        printOutcome(outcome5); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]

        OutcomePair outcome6 = get("K1", client);
        printOutcome(outcome6); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1], [SUCCESS, 3]

    }

//    public void testSHUTDOWN() throws SocketTimeoutException, UnknownHostException, InterruptedException {
//        //assertTrue( putOneGetOne());
//        UDPClient client = new UDPClient();
//        client.createSocket();
//
//        Thread.sleep(100000);
//
//        OutcomePair outcome0 = isAlive(client);
//        printOutcome(outcome0); // Expect SUCCESS
//
//        Thread.sleep(1000);
//
//        OutcomePair outcome1 = shutdown(client);
//        printOutcome(outcome1); // Expect TIMEOUT
//
//        Thread.sleep(1000);
//
//        OutcomePair outcome2 = isAlive(client);
//        printOutcome(outcome2); // Expect TIMEOUT
//    }

    // TODO
    // Get Keys as an Array of Tuple -> List requests
    // List nodes
    // Calculate throughput

}
