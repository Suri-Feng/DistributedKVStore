package com.g3.CPEN431.A9.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A9.Model.Distribution.*;
import com.g3.CPEN431.A9.Utility.MemoryUsage;
import com.g3.CPEN431.A9.Model.Store.KVStore;
import com.g3.CPEN431.A9.Model.Store.StoreCache;
import com.g3.CPEN431.A9.Model.Store.Value;
import com.g3.CPEN431.A9.Utility.StringUtils;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.CRC32;

import com.g3.CPEN431.A9.Model.Distribution.KeyTransferManager;
import com.google.protobuf.InvalidProtocolBufferException;
import org.checkerframework.checker.units.qual.A;

import static com.g3.CPEN431.A9.Utility.NetUtils.getChecksum;

public class KVServerHandler implements Runnable {
    DatagramSocket socket;
    Message.Msg requestMessage;
    InetAddress address;
    int port;
    KVStore store = KVStore.getInstance();
    StoreCache storeCache = StoreCache.getInstance();
    NodesCircle nodesCircle = NodesCircle.getInstance();
    HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();
    KeyTransferManager keyTransferManager = KeyTransferManager.getInstance();
//    Replication replication;

    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    InetAddress address,
                    int port
//                    Replication replication
    ) {
        this.socket = socket;
        this.requestMessage = requestMessage;
        this.address = address;
        this.port = port;
//        this.replication = replication;
    }

    private void manageHeartBeats(List<Long> heartbeatList) {
        heartbeatsManager.updateHeartbeats(heartbeatList);
    }

    @Override
    public void run() {
        try {
            // Request payload from client
            KeyValueRequest.KVRequest reqPayload = KeyValueRequest.KVRequest
                    .parseFrom(requestMessage.getPayload().toByteArray());

            int command = reqPayload.getCommand();

            // Receive heartbeats
            if (command == Command.HEARTBEAT.getCode()) {
//                if(KVServer.port == 12385 ) {
//                    System.out.println("12385 recv heartbeat from " + port + ", before update" +  heartbeatsManager.getHeartBeats().get(port - 12385) + ", "+ (System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(port - 12385)));
//                }
                List<Long> heartbeats = reqPayload.getHeartbeatList();

                manageHeartBeats(heartbeats);

                if((System.currentTimeMillis() - heartbeats.get(nodesCircle.getThisNodeId())) > heartbeatsManager.mostPastTime) {
//                    System.out.println(KVServer.port + " will not update node circle");
                    return;
                }
                updateNodeCircle();
//                if(KVServer.port == 12385 ) {
//                    System.out.println("12385 recv heartbeat from " + port + ", after update" + heartbeatsManager.getHeartBeats().get(port - 12385) + ", "+(System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(port - 12385)));
//                }
                return;
            }

            // Receive keys transfer
            if  (command == Command.KEY_TRANSFER.getCode()) {
                addKey(reqPayload.getPair());
                return;
            }

            // Receive notify to transfer keys to recovered node as its successor
            // send to predecessor and other 2 replica
//            if  (command == Command.SUCCESSOR_NOTIFY.getCode()) {
//                Node node = nodesCircle.getNodeById(reqPayload.getRecoveredNodeId());
//                List<KeyValueRequest.HashRange> hashRanges = reqPayload.getHashRangesList();
//                List<KeyValueRequest.KeyValueEntry> allPairs = keyTransferManager.transferKeysWithinRange(node, hashRanges);
//                Set<Node> backupNodes = nodesCircle.findSuccessorNodes(node);
//                for(Node backupNode: backupNodes) {
//                    if(backupNode != nodesCircle.getCurrentNode()) {
//                        keyTransferManager.sendMessage(allPairs, backupNode);
//                    }
//                }
//                System.gc();
////                System.out.println(KVServer.port + " is a successor of " + node.getPort());
//            }

            // reroute PUT/GET/REMOVE requests if come directly from client and don't belong to current node
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                // Find correct node and Reroute
                updateNodeCircle();
                ByteString key = reqPayload.getKey();

                if(!isPrimary(key)) {
                    reRoute(nodesCircle.findNodebyKey(key));
                    return;
                }

            }

            if (command == Command.BACKUP_WRITE.getCode()) {
                backupPUT(reqPayload);
                return;
            }

            if (command == Command.BACKUP_REM.getCode()) {
                backupREM(reqPayload);
                return;
            }

            // 1. request comes from another node who thinks I'm the right node
            // 2. request from client, but I think im the right node
            // could be true or im temporarily storing the data b/c the actual right node is down
            getResponseFromOwnNode(reqPayload);
        } catch (IOException e) {
            System.out.println("===================");
            System.out.println("[ KV Server Handler, "+socket.getLocalPort()+", " + Thread.currentThread().getName() + "]: "
                    + e.getLocalizedMessage() + e.getMessage());
            System.out.println("===================");
            throw new RuntimeException(e);
        }
    }


//    private void handleBackupNEK() {
//        replication.updateWriteAckCacheNEK(requestMessage.getMessageID());
//    }
//
//    private void handleBackupAck() throws InvalidProtocolBufferException {
//        replication.updateWriteAckCache(requestMessage.getMessageID());
//    }


        private void backupPUT(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        if (isMemoryOverload()) {
            // To client
//            replication.sendOutOfSpaceToClient(InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
//                    requestMessage.getClientPort(), requestMessage.getMessageID());
            return;
        }
//        System.out.println(KVServer.port + " received backup put: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));
        Value valueV = new Value(requestPayload.getVersion(), requestPayload.getValue());
        store.getStore().put(requestPayload.getKey(), valueV);
        //System.out.println(socket.getLocalPort() + " save: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));

        // To primary
//        replication.sendAckToPrimary(address, port, requestMessage.getMessageID());
    }

    private void backupREM(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        if (store.getStore().get(requestPayload.getKey()) != null) {
//            replication.sendNEKToPrimary(address, port, requestMessage.getMessageID());
            return;
        }
        // To primary
        store.getStore().remove(requestPayload.getKey());
//        replication.sendAckToPrimary(address, port, requestMessage.getMessageID());
    }

    private void addKey(KeyValueRequest.KeyValueEntry pair) {
//        System.out.println(KVServer.port + " received key transfer: " + StringUtils.byteArrayToHexString(pair.getKey().toByteArray()));
        store.getStore().put(
                pair.getKey(),
                new Value(pair.getVersion(), pair.getValue()));

//        byte[] key = pair.getKey().toByteArray();
//        String sha256 = Hashing.sha256()
//                .hashBytes(key).toString();
//        Node nodeMatch = nodesCircle.findCorrectNodeByHash(sha256.hashCode());
//
//        if(nodesCircle.getAliveNodesList().containsValue(nodeMatch) && nodeMatch.getId() != nodesCircle.getThisNodeId())
//        {
//            //System.out.println("send to " + nodeMatch.getPort());
//            List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
//            allPairs.add(pair);
//            KeyTransferManager.getInstance().sendMessage(allPairs, nodeMatch);
//        }

    }

    private void getResponseFromOwnNode(KeyValueRequest.KVRequest reqPayload) throws IOException {
        // If cached request, get response msg from cache and send it
        byte[] id = requestMessage.getMessageID().toByteArray();
        Message.Msg cachedResponse = storeCache.getCache().getIfPresent(ByteBuffer.wrap(id));
        if (cachedResponse != null) {
            //if (KeyValueResponse.KVResponse.parseFrom(cachedResponse.getPayload()).getErrCode() != ErrorCode.WAIT_FOR_ACK.getCode()) {
                sendResponse(cachedResponse, requestMessage);
            //}
            return; // TODO: Currently, will lose the msg if wait for ack in cache
        }

        // Prepare response payload as per client's command
        KeyValueResponse.KVResponse responsePayload = processRequest(reqPayload);

        // Attach payload, id, and checksum to reply message
        CRC32 checksum = new CRC32();
        checksum.update(id);
        checksum.update(responsePayload.toByteArray());

        Message.Msg responseMsg = Message.Msg.newBuilder()
                .setMessageID(requestMessage.getMessageID())
                .setPayload(responsePayload.toByteString())
                .setCheckSum(checksum.getValue())
                .build();

        //if ((reqPayload.getCommand() != Command.PUT.getCode()) && (reqPayload.getCommand() != Command.REMOVE.getCode())) {
            sendResponse(responseMsg, requestMessage);
        //}
        storeCache.getCache().put(ByteBuffer.wrap(id), responseMsg);
    }

    private void reRoute(Node node) throws IOException {
        Message.Msg.Builder msgBuilder = Message.Msg.newBuilder()
                    .setMessageID(requestMessage.getMessageID())
                    .setPayload(requestMessage.getPayload())
                    .setCheckSum(requestMessage.getCheckSum())
                    .setClientPort(this.port)
                    .setClientAddress(ByteString.copyFrom(this.address.getAddress()));

        byte[] responseAsByteArray = msgBuilder.build().toByteArray();
        DatagramPacket responsePkt = new DatagramPacket(
                responseAsByteArray,
                responseAsByteArray.length,
                node.getAddress(),
                node.getPort());
        socket.send(responsePkt);
    }

    private void sendResponse(Message.Msg msg, Message.Msg requestMessage) throws IOException {
        byte[] responseAsByteArray = msg.toByteArray();
        DatagramPacket responsePkt;
        if (requestMessage.hasClientAddress()) {
            responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
                    requestMessage.getClientPort());
        } else {
            responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    this.address,
                    this.port);
        }
        socket.send(responsePkt);
    }

    /*
     *  Generate a response payload
     */
    private KeyValueResponse.KVResponse processRequest(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        // Get command, key, and value from request
        int commandCode = requestPayload.getCommand();
        ByteString key = requestPayload.getKey();
        byte[] value = requestPayload.getValue().toByteArray();

        // Find corresponding Command
        Optional<Command> command = Command.findCommand(commandCode);
        KeyValueResponse.KVResponse.Builder builder = KeyValueResponse.KVResponse.newBuilder();

        if (command.isEmpty()) {
            return builder
                    .setErrCode(ErrorCode.UNKNOWN_COMMAND.getCode()).build();
        }

        if (key.size() > KVServer.MAX_KEY_LENGTH) {
            return builder
                    .setErrCode(ErrorCode.INVALID_KEY.getCode())
                    .build();
        }
        if (value.length > KVServer.MAX_VALUE_LENGTH) {
            return builder
                    .setErrCode(ErrorCode.INVALID_VALUE.getCode())
                    .build();
        }

        Boolean remove = false;

        switch (command.get()) {
            case PUT:
                if (isMemoryOverload()) {
                    return builder
                            .setErrCode(ErrorCode.OUT_OF_SPACE.getCode())
                            .build();
                }
//                replication.createWriteAckCache(requestMessage.getMessageID(),
//                        requestMessage.hasClientAddress()? InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()): address,
//                        requestMessage.hasClientPort()? requestMessage.getClientPort(): port);

                Value valueV = new Value(requestPayload.getVersion(), requestPayload.getValue());
                store.getStore().put(key, valueV);
//                System.out.println(socket.getLocalPort() + " save: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));

                sendWriteToBackups(requestPayload, requestMessage.getMessageID(), remove);

                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET:
                    Value valueInStore = store.getStore().get(key);
                    if (valueInStore == null) {
                       System.out.println(socket.getLocalPort() + " no key: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));
                        return builder
                                .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                                .build();
                    }

                    return builder
                            .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                            .setValue(valueInStore.getValue())
                            .setVersion(valueInStore.getVersion())
                            .build();
            case REMOVE:
                if (store.getStore().get(key) == null) {
                    return builder
                            .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                            .build();
                }

                store.getStore().remove(key);

                remove = true;
                sendWriteToBackups(requestPayload, requestMessage.getMessageID(), remove);

                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case SHUTDOWN:
                System.exit(0);
            case WIPE_OUT:
                wipeOut();
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case IS_ALIVE:
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET_PID:
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .setPid(KVServer.PROCESS_ID)
                        .build();
            case GET_MEMBERSHIP_COUNT:
                updateNodeCircle();
//                if(KVServer.port == 12390) {
//                    System.out.println("==========");
//                    for (Map.Entry<Integer, Node> entry: nodesCircle.getCircle().entrySet()) {
////            System.out.println(entry.getKey());
//                        System.out.println(entry.getValue().getPort());
//                    }
//                    System.out.println("==========");
//                }

                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .setMembershipCount(nodesCircle.getAliveNodesCount())
                        .build();
        }
        return builder
                .setErrCode(ErrorCode.INTERNAL_FAILURE.getCode())
                .build();
    }

    private void wipeOut() {
        store.clearStore();
        storeCache.clearCache();
        Runtime.getRuntime().freeMemory();
        System.gc();
    }

    private boolean isMemoryOverload() {
        return MemoryUsage.getFreeMemory() < 0.04 * MemoryUsage.getMaxMemory();
    }


    public void updateNodeCircle() {
        ConcurrentHashMap<Integer, Node> deadNodes = nodesCircle.getDeadNodesList();
        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
        ConcurrentHashMap<Integer, Node> allNodes = nodesCircle.getAllNodesList();
        ConcurrentHashMap<Node, List<KeyValueRequest.HashRange>> removedPrimaryHashRanges = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Node> recoveredNodes = new ConcurrentHashMap<>();
        ConcurrentSkipListMap<Integer, ConcurrentHashMap<Integer, Node>> successorNodes = nodesCircle.getMySuccessors();

//        if(KVServer.port == 12385) {
//            System.out.println("12385 my time, " + System.currentTimeMillis() + ", " + (System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(nodesCircle.getCurrentNode().getId())));
//        }

        for (Node node : allNodes.values()) {
            //if (node == nodesCircle.getCurrentNode()) continue;
            if (!heartbeatsManager.isNodeAlive(node) && aliveNodes.contains(node)) {

                //If my predecessor dead, I will take the primary postion
                // I will need my predecessor's place on the ring, before remove it
                if (nodesCircle.getMyPredessors().contains(node))
                    removedPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));

                nodesCircle.removeNode(node);

                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
                System.out.println(KVServer.port + " remove node: " + node.getPort()  + ", last update time " + time2 + ", from now past " + time);

            } else if (heartbeatsManager.isNodeAlive(node) && !aliveNodes.contains(node)) {

                nodesCircle.rejoinNode(node);
                recoveredNodes.put(node.getId(), node);

                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
                System.out.println(KVServer.port + " Adding back node: " + node.getPort() + " num servers left: "
                        + nodesCircle.getAliveNodesCount() + ", last update time " + time2 + ", from now past " + time);
            }
        }

        // If my successor changed (died or recovered), I will replace the backup
        // I will need new successor's place after ring updated
        nodesCircle.updateMySuccessor();
        NavigableSet<Integer> VNSet =  nodesCircle.getMySuccessors().keySet();
        for(int VN: VNSet) {
            for(Node currentBackup: nodesCircle.getMySuccessors().get(VN).values()) {
                if (successorNodes.get(VN) == null || !successorNodes.get(VN).contains(currentBackup))
                    updateBackupPosition(currentBackup, VN);
            }
        }

//        for(Node node: nodesCircle.getMySuccessors().values()) {
//            if(successorNodes == null || !successorNodes.contains(node)) {
//                updateBackupPosition(node);
//            }
//        }

        for(Node node: removedPrimaryHashRanges.keySet())
            takePrimaryPosition(removedPrimaryHashRanges.get(node));

        // Need the updated circle to update predecessor list
        nodesCircle.updateMyPredecessor();

        //  If my predecessor recovered, I will send keys to predecessor and two other replica (keys belong to my recovered successor)
        // I will need my predecessor's place AFTER it is added to the ring
        for(Node node: recoveredNodes.values()) {
            if (nodesCircle.getMyPredessors().contains(node)) {
                recoverPrimaryPosition(node);
            }
        }

//
//         If notify the recovered nodes for transfer
//        for (Node node: recoveredNodes) {
//            Set<Node> successorNodes = nodesCircle.findSuccessorNodes(node);
//            keyTransferManager.sendMessageToSuccessor(successorNodes, node);
//        }
    }

    private boolean keyWithinRange(KeyValueRequest.HashRange hashRange, int ringHash) {
        if(hashRange.getMinRange() <= hashRange.getMaxRange()) {
            return (ringHash >= hashRange.getMinRange()) && (ringHash <= hashRange.getMaxRange());
        }else{
            return (ringHash <= hashRange.getMaxRange()) || (ringHash >= hashRange.getMinRange());
        }
    }

    public void updateBackupPosition(Node newBackup, int VN) {
        //System.out.println("Run into update backup");
        // List<KeyValueRequest.HashRange> hashRanges = nodesCircle.getRecoveredNodeRange(nodesCircle.getCurrentNode());
        KeyValueRequest.HashRange hashRange = nodesCircle.getHashRangeByHash(VN, nodesCircle.getCurrentNode());
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

            if (keyWithinRange(hashRange, ringHash))
                allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                        .setVersion(entry.getValue().getVersion())
                        .setValue(entry.getValue().getValue())
                        .setKey(entry.getKey())
                        .build());
        }
        keyTransferManager.sendMessage(allPairs, newBackup);
    }

    public void recoverPrimaryPosition(Node recoveredPrimary) {
        NavigableSet<Integer> myVNSet =  nodesCircle.getMySuccessors().keySet();
        List<KeyValueRequest.HashRange> hashRanges = nodesCircle.getRecoveredNodeRange(recoveredPrimary);
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        // TODO: Instead of sending all keys within 3 ranges
        // I am only responsible for sending to the range with maxRange just pred to my VN(s)
        // Send to all for now
        for (KeyValueRequest.HashRange range : hashRanges) {
            //if (!myVNSet.contains(nodesCircle.getNextRingHash(range.getMaxRange()))) continue;

            for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
                String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
                int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

                if (keyWithinRange(range, ringHash)) {
                    allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                            .setVersion(entry.getValue().getVersion())
                            .setValue(entry.getValue().getValue())
                            .setKey(entry.getKey())
                            .build());
                }
            }
        }

        keyTransferManager.sendMessage(allPairs, recoveredPrimary);
//        for (Node backupNode : nodesCircle.findSuccessorNodesHashMap(recoveredPrimary).values()) {
//            if (backupNode != nodesCircle.getCurrentNode()) {
//                keyTransferManager.sendMessage(allPairs, backupNode);
//            }
//        }
    }

    //If my predecessor dead, I will take the primary postion
    // I will need my predecessor's place on the ring, before remove it
    public void takePrimaryPosition(List<KeyValueRequest.HashRange> hashRanges) {
        ConcurrentHashMap<Node, List<KeyValueRequest.KeyValueEntry>> allPairsForNode = new ConcurrentHashMap<>();
        //hashRanges.addAll(nodesCircle.getRecoveredNodeRange(nodesCircle.getCurrentNode()));

        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

            for(KeyValueRequest.HashRange range: hashRanges) {
                if (keyWithinRange(range, ringHash)) {

                    int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

                    // TODO: Only need to pass to one succ (now pass to three), but this is not the priority for now
                    for(Node currentBackup: nodesCircle.getMySuccessors().get(VN).values()) {

                            if (!allPairsForNode.containsKey(currentBackup))
                                allPairsForNode.put(currentBackup, new ArrayList<>());
                            allPairsForNode.get(currentBackup).add(KeyValueRequest.KeyValueEntry.newBuilder()
                                    .setVersion(entry.getValue().getVersion())
                                    .setValue(entry.getValue().getValue())
                                    .setKey(entry.getKey())
                                    .build());

                    }
                }
            }
        }

        for(Node newBackupNode: allPairsForNode.keySet()) {
            keyTransferManager.sendMessage(allPairsForNode.get(newBackupNode), newBackupNode);
        }
    }

    public void sendWriteToBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, Boolean remove) {
        ByteString key = reqPayload.getKey();
        String sha256 = Hashing.sha256().hashBytes(key.toByteArray()).toString();

        int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
        int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

        for (Node backupNode: nodesCircle.getMySuccessors().get(VN).values()) {

            KeyValueRequest.KVRequest request;

            if(remove)
            {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_REM.getCode())
                        .build();
            }
            else
            {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_WRITE.getCode())
                        .setKey(reqPayload.getKey())
                        .setValue(reqPayload.getValue())
                        .setVersion(reqPayload.getVersion())
                        .build();
            }

            Message.Msg requestMessage = Message.Msg.newBuilder()
                    .setMessageID(MessageID)
                    .setPayload(request.toByteString())
                    .setCheckSum(getChecksum(MessageID.toByteArray(), request.toByteArray()))
//                    .setClientAddress(ByteString.copyFrom(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientAddress().getAddress()))
//                    .setClientPort(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientPort())
                    .build();

            byte[] requestBytes = requestMessage.toByteArray();
            DatagramPacket packet = new DatagramPacket(
                    requestBytes,
                    requestBytes.length,
                    backupNode.getAddress(),
                    backupNode.getPort());

            try {
                socket.send(packet);
            } catch (IOException e) {
                System.out.println("====================");
                System.out.println("[sendWriteToBackups]" + e.getMessage());
                System.out.println("====================");
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isPrimary(ByteString key) {
        NodesCircle nodesCircle = NodesCircle.getInstance();
        return nodesCircle.findNodebyKey(key).getId() == nodesCircle.getThisNodeId();
    }
}


