//package com.g3.CPEN431.project;
//
//import com.g3.CPEN431.project.ServerInfo.Server;
//import com.g3.CPEN431.project.ServerInfo.ServerList;
//import com.g3.CPEN431.project.Test.OutcomePair;
//import com.g3.CPEN431.project.Test.TestHandler;
//import org.junit.Test;
//
//import com.g3.CPEN431.project.Client.UDPClient;
//
//import java.io.IOException;
//
//
//public class TestShutDown {
//    @Test
//    public void ShutDown() throws IOException, InterruptedException {
//        TestHandler testHandler = new TestHandler();
//        UDPClient client = testHandler.clientList.getFirstClient();
//        Server server = testHandler.serverList.getFirstServer();
//
//        OutcomePair outcome0 = testHandler.isAlive(client, server);
//        testHandler.printOutcome(outcome0); // Expect SUCCESS
//
//        Thread.sleep(1000);
//
//        OutcomePair outcome1 = testHandler.shutdown(client, server);
//        testHandler.printOutcome(outcome1); // Expect TIMEOUT
//
//        Thread.sleep(1000);
//
//        OutcomePair outcome2 = testHandler.isAlive(client, server);
//        testHandler.printOutcome(outcome2); // Expect TIMEOUT
//    }
//}
