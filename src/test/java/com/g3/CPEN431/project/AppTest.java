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

        wipeOut(client);
        Thread.sleep(1000);
        // PUT->PUT->REM
//        OutcomePair outcome0 = get("K1", client);
//        System.out.println("GET [K1]");
//        printOutcome(outcome0); // EXPECT GET(K1) -> [TIMEOUT, NULL], [K-N-F, NULL]

        //System.out.println("PUT [K1, 1, 0]");
        OutcomePair outcome1 = put("K1", "1", 0, client);
        printOutcome(outcome1); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]

       // System.out.println("GET [K1]");
        OutcomePair outcome2 = get("K1", client);
        printOutcome(outcome2); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1]

        //System.out.println("PUT [K1, 2, 1]");
        OutcomePair outcome3 = put("K1", "2", 1, client);
        printOutcome(outcome3); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]

        //System.out.println("GET [K1]");
        OutcomePair outcome4 = get("K1", client);
        printOutcome(outcome4); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1], [SUCCESS, 3]

        //System.out.println("REM [K1]");
        OutcomePair outcome5 = remove("K1", client);
        printOutcome(outcome5); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]

        //System.out.println("GET [K1]");
        OutcomePair outcome6 = get("K1", client);
        printOutcome(outcome6); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1], [SUCCESS, 3]

    }

    // TODO
    // Get Keys as an Array of Tuple
    // More commands: shut down -> isAlive
    // More nodes

}
