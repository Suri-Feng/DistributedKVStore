package com.g3.CPEN431.A11.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A11.Model.Distribution.*;
import com.g3.CPEN431.A11.Model.Store.QueuedMessage;
import com.g3.CPEN431.A11.Utility.MemoryUsage;
import com.g3.CPEN431.A11.Model.Store.KVStore;
import com.g3.CPEN431.A11.Model.Store.StoreCache;
import com.g3.CPEN431.A11.Model.Store.Value;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;
import com.g3.CPEN431.A11.Model.Distribution.KeyTransferManager;

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
    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    InetAddress address,
                    int port
    ) {
        this.socket = socket;
        this.requestMessage = requestMessage;
        this.address = address;
        this.port = port;
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

            if (command == Command.PUT_ACK.getCode()) {
                processBackupAck();
                return;
            }

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

    private void processBackupAck() throws IOException {
        QueuedMessage queuedMessage = storeCache.getQueuedResponses().getIfPresent(requestMessage.getMessageID());

        if (queuedMessage != null) {
            byte[] id = queuedMessage.getId().toByteArray();

            queuedMessage.addAckPort(this.port);

            if (queuedMessage.getAckPortsCount() == 3) {
                KeyValueResponse.KVResponse responsePayload = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
                CRC32 checksum = new CRC32();

                checksum.update(id);
                checksum.update(responsePayload.toByteArray());

                Message.Msg responseMsg = Message.Msg.newBuilder()
                        .setMessageID(queuedMessage.getId())
                        .setPayload(responsePayload.toByteString())
                        .setCheckSum(checksum.getValue())
                        .build();

                if (storeCache.getCache().getIfPresent(ByteString.copyFrom(id)) == null) {
                    storeCache.getCache().put(ByteString.copyFrom(id), responseMsg);
                    storeCache.getQueuedResponses().invalidate(requestMessage.getMessageID());
                    sendResponse(responseMsg, queuedMessage.getAddress(), queuedMessage.getPort());
                }
            }
        }
    }

    private void getResponseFromOwnNode(KeyValueRequest.KVRequest reqPayload) throws IOException {
        // If cached request, get response msg from cache and send it
        byte[] id = requestMessage.getMessageID().toByteArray();
        Message.Msg cachedResponse = storeCache.getCache().getIfPresent(ByteString.copyFrom(id));
        if (cachedResponse != null) {
            sendResponse(cachedResponse, requestMessage);
            return;
        }

        // Prepare response payload as per client's command
        KeyValueResponse.KVResponse responsePayload = processRequest(reqPayload);

        if (nodesCircle.getStartupNodesSize() != 1 && reqPayload.getCommand() == Command.PUT.getCode()
                && responsePayload.getErrCode() == ErrorCode.SUCCESSFUL.getCode()) {
            String uuid = UUID.randomUUID().toString();
            QueuedMessage queuedMessage;
            if (!requestMessage.hasClientPort()) {
                queuedMessage = new QueuedMessage(this.address, this.port, requestMessage.getMessageID(), reqPayload.getKey(), reqPayload.getValue(), reqPayload.getVersion());
            } else {
                queuedMessage = new QueuedMessage(InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
                        requestMessage.getClientPort(),
                        requestMessage.getMessageID(),
                        reqPayload.getKey(),
                        reqPayload.getValue(),
                        reqPayload.getVersion());
            }
            long R_TS = System.currentTimeMillis();
            putValueInStore(reqPayload.getKey(), reqPayload.getValue(), reqPayload.getVersion(), R_TS);
            storeCache.getQueuedResponses().put(ByteString.copyFromUtf8(uuid), queuedMessage);
            sendWriteToBackups(reqPayload, ByteString.copyFromUtf8(uuid), false, R_TS);
            return;
        }
        // Attach payload, id, and checksum to reply message
//        long R_TS = System.currentTimeMillis();
//        boolean sendWriteToBackups = false;
//        if (reqPayload.getCommand() == Command.PUT.getCode() && responsePayload.getErrCode() == ErrorCode.SUCCESSFUL.getCode()) {
//            if (nodesCircle.getStartupNodesSize() == 1) {
//                store.getStore().put(reqPayload.getKey(), new Value(reqPayload.getVersion(), reqPayload.getValue()));
//            } else {
//                sendWriteToBackups = true;
//                putValueInStore(reqPayload.getKey(), reqPayload.getValue(), reqPayload.getVersion(), R_TS);
//            }
//        }

        CRC32 checksum = new CRC32();
        checksum.update(id);
        checksum.update(responsePayload.toByteArray());

        Message.Msg responseMsg = Message.Msg.newBuilder()
                .setMessageID(requestMessage.getMessageID())
                .setPayload(responsePayload.toByteString())
                .setCheckSum(checksum.getValue())
                .build();

        sendResponse(responseMsg, requestMessage);
        storeCache.getCache().put(ByteString.copyFrom(id), responseMsg);

        if (nodesCircle.getStartupNodesSize() != 1 &&
                reqPayload.getCommand() == Command.REMOVE.getCode() && responsePayload.getErrCode() == ErrorCode.SUCCESSFUL.getCode()) {
            sendWriteToBackups(reqPayload, requestMessage.getMessageID(), true, 0);
        }
//        if (sendWriteToBackups) {
//            sendWriteToBackups(reqPayload, requestMessage.getMessageID(), false, R_TS);
//        }
    }

    private void sendResponse(Message.Msg msg, InetAddress address, int port) throws IOException {
        byte[] responseAsByteArray = msg.toByteArray();
        DatagramPacket responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    address,
                    port);
        socket.send(responsePkt);
    }

    private void putValueInStore(ByteString key, ByteString value, int version, long R_TS) {
        store.getStore().put(key, new Value(version, value, R_TS));
    }

    private void backupPUT(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        if (isMemoryOverload()) {
            return;
        }
        Value inStoreValue = store.getStore().get(requestPayload.getKey());
        if (inStoreValue != null && inStoreValue.getR_TS() > requestPayload.getRTS()) {
            return;
        }

        putValueInStore(requestPayload.getKey(), requestPayload.getValue(), requestPayload.getVersion(), requestPayload.getRTS());

        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Command.PUT_ACK.getCode())
                .build();

        Message.Msg requestMessage = Message.Msg.newBuilder()
                .setMessageID(this.requestMessage.getMessageID())
                .setPayload(request.toByteString())
                .setCheckSum(0)
                .build();

        byte[] requestBytes = requestMessage.toByteArray();
        DatagramPacket packet = new DatagramPacket(
                requestBytes,
                requestBytes.length,
                this.address,
                this.port);

        try {
            socket.send(packet);
        } catch (IOException e) {
            System.out.println("====================");
            System.out.println("[sendACKtoPrimary]" + e.getMessage());
            System.out.println("====================");
            throw new RuntimeException(e);
        }
    }

    private void backupREM(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        store.getStore().remove(requestPayload.getKey());
    }

    private void addKey(KeyValueRequest.KeyValueEntry pair) {
        Value inStoreValue = store.getStore().get(pair.getKey());
        if (inStoreValue != null && inStoreValue.getR_TS() > pair.getRTS()) {
            return;
        }
        Value value = new Value(pair.getVersion(), pair.getValue(), pair.getRTS());
        store.getStore().put(
                pair.getKey(),
                value);
    }

    private void addKeyPrimaryRecover(KeyValueRequest.KeyValueEntry pair) {
        if (store.getStore().containsKey(pair.getKey()) && store.getStore().get(pair.getKey()).getR_TS() > pair.getRTS()) {
            return;
        }

        Value value = new Value(pair.getVersion(), pair.getValue(), pair.getRTS());

        store.getStore().put(
                pair.getKey(),
                value);

        byte[] key = pair.getKey().toByteArray();
        String sha256 = Hashing.sha256()
                .hashBytes(key).toString();
        Node nodeMatch = nodesCircle.findCorrectNodeByHash(sha256.hashCode());

        if (nodesCircle.getAliveNodesList().containsValue(nodeMatch) && nodeMatch.getId() != nodesCircle.getThisNodeId()) {
            List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
            allPairs.add(pair);
            keyTransferManager.sendMessagePrimaryRecover(allPairs, nodeMatch);
        }

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
    private KeyValueResponse.KVResponse processRequest(KeyValueRequest.KVRequest requestPayload) {
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
                if (nodesCircle.getStartupNodesSize() == 1) {
                    store.getStore().put(requestPayload.getKey(), new Value(requestPayload.getVersion(), requestPayload.getValue()));
                }
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET:
                Value valueInStore = store.getStore().get(key);
                if (valueInStore == null) {
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
        if (nodesCircle.getStartupNodesSize() == 1) {
            return MemoryUsage.getFreeMemory() < 0.035 * MemoryUsage.getMaxMemory();
        } else {
            return MemoryUsage.getFreeMemory() < 0.04 * MemoryUsage.getMaxMemory();
        }
    }


    public void updateNodeCircle() {
        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
        ConcurrentHashMap<Integer, Node> allNodes = nodesCircle.getAllNodesList();
        ConcurrentHashMap<Node, List<KeyValueRequest.HashRange>> removedPrimaryHashRanges = new ConcurrentHashMap<>();
        ConcurrentHashMap<Node, List<KeyValueRequest.HashRange>> recoveredPrimaryHashRanges = new ConcurrentHashMap<>();

        for (Node node : allNodes.values()) {
            if (!heartbeatsManager.isNodeAlive(node) && aliveNodes.contains(node)) {

                // If my predecessor dead, I will take the primary postion
                // I will need my predecessor's place on the ring, before remove it
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node))
                        removedPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));
                }

                nodesCircle.removeNode(node);
            } else if (heartbeatsManager.isNodeAlive(node) && !aliveNodes.contains(node)) {

                nodesCircle.rejoinNode(node);
                boolean contains = false;
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node)) contains = true;
                }
                if (!contains)
                    recoveredPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));
            }
        }

        // If my successor changed (died or recovered), I will replace the backup
        // I will need new successor's place after ring updated
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> newSuccsForVNs = nodesCircle.updateMySuccessor();
        for (Integer VN : newSuccsForVNs.keySet()) {
            for (Node newBackup : newSuccsForVNs.get(VN).values()) {
                updateBackupPosition(newBackup, VN);
            }
        }


        for (Node node : removedPrimaryHashRanges.keySet())
            takePrimaryPosition(removedPrimaryHashRanges.get(node));

        //  If my predecessor recovered, I will send keys to predecessor and two other replica (keys belong to my recovered successor)
        // I will need my predecessor's place AFTER it is added to the ring
        for (Node node : recoveredPrimaryHashRanges.keySet()) {
            recoverPrimaryPosition(node, recoveredPrimaryHashRanges.get(node));
        }
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
                        .build());
        }
        keyTransferManager.sendMessage(allPairs, newBackup);
    }

    public void recoverPrimaryPosition(Node recoveredPrimary, List<KeyValueRequest.HashRange> hashRanges) {
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        // I am only responsible for sending to the range with maxRange just pred to my VN(s)
        for (KeyValueRequest.HashRange range : hashRanges) {
            for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
                String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
                int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

                if (keyWithinRange(range, ringHash)) {
                    allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                            .setVersion(entry.getValue().getVersion())
                            .setValue(entry.getValue().getValue())
                            .setKey(entry.getKey())
                            .setRTS(entry.getValue().getR_TS())
                            .build());
                }
            }
        }

        keyTransferManager.sendMessagePrimaryRecover(allPairs, recoveredPrimary);
    }

    //If my predecessor dead, I will take the primary postion
    // I will need my predecessor's place on the ring, before remove it
    public void takePrimaryPosition(List<KeyValueRequest.HashRange> hashRanges) {
        ConcurrentHashMap<Node, List<KeyValueRequest.KeyValueEntry>> allPairsForNode = new ConcurrentHashMap<>();

        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

            for (KeyValueRequest.HashRange range : hashRanges) {
                if (keyWithinRange(range, ringHash)) {

                    int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

                    for (Node currentBackup : nodesCircle.getMySuccessors().get(VN).values()) {

                        if (!allPairsForNode.containsKey(currentBackup))
                            allPairsForNode.put(currentBackup, new ArrayList<>());
                        allPairsForNode.get(currentBackup).add(KeyValueRequest.KeyValueEntry.newBuilder()
                                .setVersion(entry.getValue().getVersion())
                                .setValue(entry.getValue().getValue())
                                .setKey(entry.getKey())
                                .setRTS(entry.getValue().getR_TS())
                                .build());

                    }
                }
            }
        }

        for (Node newBackupNode : allPairsForNode.keySet()) {
            keyTransferManager.sendMessage(allPairsForNode.get(newBackupNode), newBackupNode);
        }
    }

    public void sendWriteToBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, Boolean remove, long R_TS) {
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
                        .setKey(reqPayload.getKey())
                        .build();
            } else {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_WRITE.getCode())
                        .setKey(reqPayload.getKey())
                        .setValue(reqPayload.getValue())
                        .setVersion(reqPayload.getVersion())
                        .setRTS(R_TS)
                        .build();
            }

            Message.Msg requestMessage = Message.Msg.newBuilder()
                    .setMessageID(MessageID)
                    .setPayload(request.toByteString())
                    .setCheckSum(0)
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


