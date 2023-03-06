//package com.g3.CPEN431.project;
//
//import com.g3.CPEN431.project.Client.UDPClient;
//import com.g3.CPEN431.project.ServerInfo.Server;
//import com.g3.CPEN431.project.Test.OutcomePair;
//import com.g3.CPEN431.project.Test.TestHandler;
//import org.junit.Test;
//
//import java.io.IOException;
//public class TestCorrectness {
//    @Test // PUT->PUT->REM
//    public void OneClientOneServer() throws IOException, InterruptedException {
//
//        TestHandler testHandler = new TestHandler();
//        UDPClient client = testHandler.clientList.getFirstClient();
//        Server server = testHandler.serverList.getFirstServer();
//
////        wipeOut(client);
////        Thread.sleep(1000);
//        OutcomePair outcome0 = testHandler.get(client, server, "K1");
//        testHandler.printOutcome(outcome0); // EXPECT GET(K1) -> [TIMEOUT, NULL], [K-N-F, NULL]
//
//        OutcomePair outcome1 = testHandler.put(client, server, "K1", "1", 0);
//        testHandler.printOutcome(outcome1); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]
//
//        OutcomePair outcome2 = testHandler.get(client, server, "K1");
//        testHandler.printOutcome(outcome2); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1]
//
//        OutcomePair outcome3 = testHandler.put(client, server, "K1", "2", 1);
//        testHandler.printOutcome(outcome3); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]
//
//        OutcomePair outcome4 = testHandler.get(client, server, "K1");
//        testHandler.printOutcome(outcome4); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1], [SUCCESS, 3]
//
//        OutcomePair outcome5 = testHandler.remove(client, server, "K1");
//        testHandler.printOutcome(outcome5); // EXPECT [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, NULL]
//
//        OutcomePair outcome6 = testHandler.get(client, server, "K1");
//        testHandler.printOutcome(outcome6); // EXPECT GET(K1) -> [K-N-F, NULL], [TIMEOUT, NULL], [SUCCESS, 1], [SUCCESS, 3]
//
//    }
//}
