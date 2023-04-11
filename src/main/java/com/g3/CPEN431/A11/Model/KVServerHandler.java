package com.g3.CPEN431.A11.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A11.Model.Distribution.*;
import com.g3.CPEN431.A11.Utility.MemoryUsage;
import com.g3.CPEN431.A11.Model.Store.KVStore;
import com.g3.CPEN431.A11.Model.Store.StoreCache;
import com.g3.CPEN431.A11.Model.Store.Value;
import com.g3.CPEN431.A11.Utility.StringUtils;
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
import java.util.zip.CRC32;

import com.g3.CPEN431.A11.Model.Distribution.KeyTransferManager;

import static com.g3.CPEN431.A11.Utility.NetUtils.getChecksum;

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
    long enter_system_time;

    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    InetAddress address,
                    int port
    ) {
        this.socket = socket;
        this.requestMessage = requestMessage;
        this.address = address;
        this.port = port;
        if (requestMessage.hasTime()) {
            this.enter_system_time = requestMessage.getTime();
        } else {
            this.enter_system_time = System.currentTimeMillis();
        }
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

            if (nodesCircle.getStartupNodesSize() == 1) {
                getResponseFromOwnNode(reqPayload);
                return;
            }

            int command = reqPayload.getCommand();

            // Receive heartbeats
            if (command == Command.HEARTBEAT.getCode()) {
                List<Long> heartbeats = reqPayload.getHeartbeatList();

                manageHeartBeats(heartbeats);

                if ((System.currentTimeMillis() - heartbeats.get(nodesCircle.getThisNodeId())) > heartbeatsManager.mostPastTime) {
                    return;
                }
                updateNodeCircle();
                return;
            }

            // Receive keys transfer
            if (command == Command.KEY_TRANSFER.getCode()) {
                addKey(reqPayload.getPair());
                return;
            }

            if (command == Command.PRIMARY_RECOVER.getCode()) {
                addKeyPrimaryRecover(reqPayload.getPair());
                return;
            }


            // reroute PUT/GET/REMOVE requests if come directly from client and don't belong to current node
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                ByteString key = reqPayload.getKey();

                if (!isPrimary(key)) {
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
            System.out.println("[ KV Server Handler, " + socket.getLocalPort() + ", " + Thread.currentThread().getName() + "]: "
                    + e.getLocalizedMessage() + e.getMessage());
            System.out.println("===================");
            throw new RuntimeException(e);
        }
    }

    private boolean putValueInStore(KeyValueRequest.KVRequest requestPayload) {
        ByteString key = requestPayload.getKey();
        Value previousValue = store.getStore().get(key);
        if (previousValue == null || (previousValue.getR_TS() <= enter_system_time && previousValue.getW_TS() <= enter_system_time)) {
            Value value = new Value(requestPayload.getVersion(), requestPayload.getValue(), enter_system_time);
            store.getStore().put(key, value);
            return true;
        }
        return false;
    }


    private void backupPUT(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        if (isMemoryOverload()) {
            return;
        }
//        System.out.println(KVServer.port + " received backup put: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));
        putValueInStore(requestPayload);
    }

    private void backupREM(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        if (store.getStore().get(requestPayload.getKey()) != null) {
            return;
        }
        // To primary
        store.getStore().remove(requestPayload.getKey());
    }

    private void addKey(KeyValueRequest.KeyValueEntry pair) {
//        System.out.println(KVServer.port + " received key transfer from "  + port +": " + StringUtils.byteArrayToHexString(pair.getKey().toByteArray()));
        Value value = new Value(pair.getVersion(), pair.getValue(), pair.getWTS());
        value.setR_TS(pair.getWTS());
        store.getStore().put(
                pair.getKey(),
                value);
    }

    private void addKeyPrimaryRecover(KeyValueRequest.KeyValueEntry pair) {
//        System.out.println(KVServer.port + " received key transfer (primary) from "  + port +": " + StringUtils.byteArrayToHexString(pair.getKey().toByteArray()));
        Value value = new Value(pair.getVersion(), pair.getValue(), pair.getWTS());
        value.setR_TS(pair.getWTS());
        store.getStore().put(
                pair.getKey(),
                value);

        byte[] key = pair.getKey().toByteArray();
        String sha256 = Hashing.sha256()
                .hashBytes(key).toString();
        Node nodeMatch = nodesCircle.findCorrectNodeByHash(sha256.hashCode());

        if (nodesCircle.getAliveNodesList().containsValue(nodeMatch) && nodeMatch.getId() != nodesCircle.getThisNodeId()) {
            //System.out.println("send to " + nodeMatch.getPort());
            List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
            allPairs.add(pair);
            keyTransferManager.sendMessagePrimaryRecover(allPairs, nodeMatch);
        }

    }

    private void getResponseFromOwnNode(KeyValueRequest.KVRequest reqPayload) throws IOException {
        // If cached request, get response msg from cache and send it
        byte[] id = requestMessage.getMessageID().toByteArray();
        Message.Msg cachedResponse = storeCache.getCache().getIfPresent(ByteBuffer.wrap(id));
        if (cachedResponse != null) {
            sendResponse(cachedResponse, requestMessage);
            return;
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

        sendResponse(responseMsg, requestMessage);
        storeCache.getCache().put(ByteBuffer.wrap(id), responseMsg);
    }

    private void reRoute(Node node) throws IOException {
        Message.Msg.Builder msgBuilder = Message.Msg.newBuilder()
                .setMessageID(requestMessage.getMessageID())
                .setPayload(requestMessage.getPayload())
                .setCheckSum(requestMessage.getCheckSum())
                .setTime(this.enter_system_time)
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

        switch (command.get()) {
            case PUT:
                if (isMemoryOverload()) {
                    return builder
                            .setErrCode(ErrorCode.OUT_OF_SPACE.getCode())
                            .build();
                }
                if (putValueInStore(requestPayload)) {
                    if (nodesCircle.getStartupNodesSize() != 1)
                        sendWriteToBackups(requestPayload, requestMessage.getMessageID(), false);

                    return builder
                            .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                            .build();
                } else {
                    System.out.println("Key " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()) + " write reject");
                    return builder
                            .setErrCode(ErrorCode.OVERLOAD.getCode())
                            .setOverloadWaitTime(50)
                            .build();
                }
            case GET:
                Value valueInStore = store.getStore().get(key);
                if (valueInStore == null) {
//                       System.out.println(socket.getLocalPort() + " no key: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));
                    return builder
                            .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                            .build();
                }
                if (valueInStore.getW_TS() > enter_system_time) {
                    // TODO: reject
                    System.out.println("Key " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()) + " read reject");
                    return builder
                            .setErrCode(ErrorCode.OVERLOAD.getCode())
                            .setOverloadWaitTime(50)
                            .build();
                } else {
                    valueInStore.setR_TS(enter_system_time);
                    return builder
                            .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                            .setValue(valueInStore.getValue())
                            .setVersion(valueInStore.getVersion())
                            .build();
                }
            case REMOVE:
                if (store.getStore().get(key) == null) {
                    return builder
                            .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                            .build();
                }

                store.getStore().remove(key);
                if (nodesCircle.getStartupNodesSize() != 1)
                    sendWriteToBackups(requestPayload, requestMessage.getMessageID(), true);

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
                if (nodesCircle.getStartupNodesSize() == 1)
                    return builder
                            .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                            .setMembershipCount(1)
                            .build();
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
        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
        ConcurrentHashMap<Integer, Node> allNodes = nodesCircle.getAllNodesList();
        ConcurrentHashMap<Node, List<KeyValueRequest.HashRange>> removedPrimaryHashRanges = new ConcurrentHashMap<>();
        ConcurrentHashMap<Node, List<KeyValueRequest.HashRange>> recoveredPrimaryHashRanges = new ConcurrentHashMap<>();
//        ConcurrentHashMap<Integer, Node> recoveredNodes = new ConcurrentHashMap<>();
//        ConcurrentSkipListMap<Integer, ConcurrentHashMap<Integer, Node>> successorNodes = nodesCircle.getMySuccessors();

//        if(KVServer.port == 12385) {
//            System.out.println("12385 my time, " + System.currentTimeMillis() + ", " + (System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(nodesCircle.getCurrentNode().getId())));
//        }

        for (Node node : allNodes.values()) {
            //if (node == nodesCircle.getCurrentNode()) continue;
            if (!heartbeatsManager.isNodeAlive(node) && aliveNodes.contains(node)) {

                //If my predecessor dead, I will take the primary postion
                // I will need my predecessor's place on the ring, before remove it
                // TODO: Optimize this
                boolean contains = false;
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node)) contains = true;
                }
                if (!contains)
                    removedPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));

                nodesCircle.removeNode(node);

//                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
//                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
//                System.out.println(KVServer.port + " remove node: " + node.getPort()  + ", last update time " + time2 + ", from now past " + time);

            } else if (heartbeatsManager.isNodeAlive(node) && !aliveNodes.contains(node)) {

                nodesCircle.rejoinNode(node);
                // TODO: The same thing as removed primary -> to get range here, since range might have been changed when send msg NO
//                recoveredNodes.put(node.getId(), node);
                boolean contains = false;
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node)) contains = true;
                }
                if (!contains)
                    recoveredPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));

//                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
//                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
//                System.out.println(KVServer.port + " Adding back node: " + node.getPort() + " num servers left: "
//                        + nodesCircle.getAliveNodesCount() + ", last update time " + time2 + ", from now past " + time);
            }
        }

        // If my successor changed (died or recovered), I will replace the backup
        // I will need new successor's place after ring updated
        //TODO: Here might be concurrent issue
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> newSuccsForVNs = nodesCircle.updateMySuccessor();
        for (Integer VN : newSuccsForVNs.keySet()) {
            for (Node newBackup : newSuccsForVNs.get(VN).values()) {
                updateBackupPosition(newBackup, VN);
            }
        }


        for (Node node : removedPrimaryHashRanges.keySet())
            takePrimaryPosition(removedPrimaryHashRanges.get(node));

        // Need the updated circle to update predecessor list
//        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> newPredsForVNs = nodesCircle.updateMyPredecessor();
//        for(Integer VN: newPredsForVNs.keySet()) {
//            for (Node newPrimary : newPredsForVNs.get(VN).values()) {
//                recoverPrimaryPosition(newPrimary, VN);
//            }
//        }

        //  If my predecessor recovered, I will send keys to predecessor and two other replica (keys belong to my recovered successor)
        // I will need my predecessor's place AFTER it is added to the ring
        for (Node node : recoveredPrimaryHashRanges.keySet()) {
            recoverPrimaryPosition(node, recoveredPrimaryHashRanges.get(node));
        }
//        for (Node node: newPreds.values()) {
//            recoverPrimaryPosition(node);
//        }
//        for(Node node: recoveredNodes.values()) {
//            if (nodesCircle.getMyPredessors().contains(node)) {
//                recoverPrimaryPosition(node);
//            }
//        }
        nodesCircle.updateMyPredecessor();
//
    }

    private boolean keyWithinRange(KeyValueRequest.HashRange hashRange, int ringHash) {
        if (hashRange.getMinRange() <= hashRange.getMaxRange()) {
            return (ringHash >= hashRange.getMinRange()) && (ringHash <= hashRange.getMaxRange());
        } else {
            return (ringHash <= hashRange.getMaxRange()) || (ringHash >= hashRange.getMinRange());
        }
    }

    public void updateBackupPosition(Node newBackup, int VN) {
//        System.out.println(KVServer.port + "sent key transfer to backup" + newBackup.getPort());
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
                        .setRTS(entry.getValue().getR_TS())
                        .setWTS(entry.getValue().getW_TS())
                        .build());
        }
        keyTransferManager.sendMessage(allPairs, newBackup);
    }

    public void recoverPrimaryPosition(Node recoveredPrimary, List<KeyValueRequest.HashRange> hashRanges) {
//        System.out.println(KVServer.port + "sent key transfer to recovered primary" + recoveredPrimary.getPort());
//        NavigableSet<Integer> myVNSet =  nodesCircle.getMySuccessors().keySet();
//        List<KeyValueRequest.HashRange> hashRanges = nodesCircle.getRecoveredNodeRange(recoveredPrimary);
//        int recoveredPrimaryVN = nodesCircle.getPrevRingHash(VN);
//        KeyValueRequest.HashRange hashRange = nodesCircle.getHashRangeByHash(recoveredPrimaryVN, recoveredPrimary);
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        // TODO: Instead of sending all keys within 3 ranges
        // I am only responsible for sending to the range with maxRange just pred to my VN(s)
        for (KeyValueRequest.HashRange range : hashRanges) {
//            if (!myVNSet.contains(nodesCircle.getNextRingHash(range.getMaxRange()))) continue;

            for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
                String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
                int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

                if (keyWithinRange(range, ringHash)) {
                    allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                            .setVersion(entry.getValue().getVersion())
                            .setValue(entry.getValue().getValue())
                            .setKey(entry.getKey())
                            .setWTS(entry.getValue().getW_TS())
                            .setRTS(entry.getValue().getR_TS())
                            .build());
                }
            }
        }

        keyTransferManager.sendMessagePrimaryRecover(allPairs, recoveredPrimary);
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

            for (KeyValueRequest.HashRange range : hashRanges) {
                if (keyWithinRange(range, ringHash)) {

                    int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

                    // TODO: Only need to pass to one succ (now pass to three), but this is not the priority for now
                    for (Node currentBackup : nodesCircle.getMySuccessors().get(VN).values()) {

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

        for (Node newBackupNode : allPairsForNode.keySet()) {
            keyTransferManager.sendMessage(allPairsForNode.get(newBackupNode), newBackupNode);
        }
    }

    public void sendWriteToBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, Boolean remove) {
        if (nodesCircle.getStartupNodesSize() == 1) return;
        ByteString key = reqPayload.getKey();
        String sha256 = Hashing.sha256().hashBytes(key.toByteArray()).toString();

        int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
        int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

        for (Node backupNode : nodesCircle.getMySuccessors().get(VN).values()) {

            KeyValueRequest.KVRequest request;

            if (remove) {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_REM.getCode())
                        .build();
            } else {
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
                    .setTime(this.enter_system_time)
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


