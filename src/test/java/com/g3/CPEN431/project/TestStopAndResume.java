//package com.g3.CPEN431.project;
//
//import com.g3.CPEN431.project.Client.UDPClient;
//import com.g3.CPEN431.project.ServerInfo.Server;
//import com.g3.CPEN431.project.Test.OutcomePair;
//import com.g3.CPEN431.project.Test.TestHandler;
//import org.junit.Test;
//
//import java.io.IOException;
//
//public class TestStopAndResume {
//
//    @Test
//    public void StopAndResume() throws IOException, InterruptedException {
//        TestHandler testHandler = new TestHandler();
//        UDPClient client = testHandler.clientList.getFirstClient();
//        Server server = testHandler.serverList.getFirstServer();
//
//        System.out.println("address" + server.getAddress());
//        System.out.println("port" + server.getPort());
//
//        OutcomePair outcome0 = testHandler.isAlive(client, server);
//        testHandler.printOutcome(outcome0); // Expect SUCCESS
//
//        OutcomePair outcome1 = testHandler.getPid(client, server);
//        testHandler.printOutcome(outcome1); // Expect SUCCESS
//
//        Thread.sleep(1000);
//
//        // TODO: Will this send a shut down request?
//        OutcomePair outcome2 = testHandler.processControlShutDown(client, server);
//        testHandler.printOutcome(outcome2); // Expect SUCCESS
//
//        OutcomePair outcome3 = testHandler.isAlive(client, server);
//        testHandler.printOutcome(outcome3); // Expect TIMEOUT
//
//        Thread.sleep(1000);
//
//        OutcomePair outcome4 = testHandler.processControlResume(client, server);
//        testHandler.printOutcome(outcome4); // Expect SUCCESS
//
//        OutcomePair outcome5 = testHandler.isAlive(client, server);
//        testHandler.printOutcome(outcome5); // Expect SUCCESS
//    }
//
//    @Test
//    public void StopAndResumeWithGet() throws IOException, InterruptedException {
//        TestHandler testHandler = new TestHandler();
//        UDPClient client = testHandler.clientList.getFirstClient();
//        Server server = testHandler.serverList.getFirstServer();
//
//        OutcomePair outcome1 = testHandler.getPid(client, server);
//        testHandler.printOutcome(outcome1); // Expect SUCCESS
//
//        OutcomePair outcome2 = testHandler.put(client, server, "K1", "1", 0);
//        testHandler.printOutcome(outcome2); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]
//        OutcomePair outcome3 = testHandler.get(client, server, "K1");
//        testHandler.printOutcome(outcome3); // EXPECT GET(K1) -> [TIMEOUT, NULL], [K-N-F, NULL]
//
//        OutcomePair outcome4 = testHandler.processControlShutDown(client, server);
//        testHandler.printOutcome(outcome4); // Expect TIMEOUT
//        OutcomePair outcome5 = testHandler.get(client, server, "K1");
//        testHandler.printOutcome(outcome5); // EXPECT TIMEOUT
//
//        Thread.sleep(1000);
//
//        OutcomePair outcome6 = testHandler.processControlResume(client, server);
//        testHandler.printOutcome(outcome6); // Expect TIMEOUT
//        OutcomePair outcome7 = testHandler.get(client, server, "K1");
//        testHandler.printOutcome(outcome7); // EXPECT GET(K1) -> [TIMEOUT, NULL], [K-N-F, NULL]
//    }
// }
