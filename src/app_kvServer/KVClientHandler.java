package app_kvServer;

import app_kvServer.storage.IKVStorage;
import org.apache.log4j.Logger;
import shared.Constants;
import shared.Hash.IHashRing;
import shared.KVMessageFactory;
import shared.ServerInfo;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.buffer.IKVMessageBufferReader;
import shared.messages.buffer.IKVMessageBufferWriter;
import shared.messages.buffer.KVMessageCrLfBufferReader;
import shared.messages.buffer.KVMessageCrLfBufferWriter;
import shared.services.KVService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class KVClientHandler implements Runnable {

    private static final Logger logger = Logger.getRootLogger();
    private final Socket clientSocket;
    private final IKVServer kvServer;

    private InputStream input;
    private OutputStream output;
    private IKVMessageBufferWriter bufferWriter;
    private boolean isOpen;

    private IHashRing metadata;
    public KVClientHandler(IHashRing metadata, Socket clientSocket, IKVServer kvServer){
        this.metadata = metadata;
        this.clientSocket = clientSocket;
        this.kvServer = kvServer;
        isOpen = true;
    }


    @Override
    public void run() {
        try {

            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            bufferWriter = new KVMessageCrLfBufferWriter(output);
            IKVMessageBufferReader bufferReader = new KVMessageCrLfBufferReader(clientSocket, input);

			/*
			sendMessage(new TextMessage(
					"Connection to MSRG Echo server established: "
							+ clientSocket.getLocalAddress() + " / "
							+ clientSocket.getLocalPort()));
			 */
            while (isOpen) {
                try {
                    IKVMessage message = bufferReader.readNextMessage();
                    messageHandler(message);
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    //logger.info("Client Connection lost!");
                    isOpen = false;
                }
            }
        } catch (IOException e) {
            logger.error("Error! Connection could not be established!", e);
        } finally {
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }


    private void messageHandler(IKVMessage message) throws IOException {
        IKVMessage.StatusType statusType = message.getStatus();

        if (statusType == IKVMessage.StatusType.GET){
            if (!validateMessage(message)) {
                IKVMessage failedMessage = KVMessageFactory.createFailedMessage("The message format is invalid");
                bufferWriter.sendMessage(failedMessage);
                return;
            }
            handleGetMessage(message);
        }
        else if (statusType == IKVMessage.StatusType.PUT){
            if (!validateMessage(message)) {
                IKVMessage failedMessage = KVMessageFactory.createFailedMessage("The message format is invalid");
                bufferWriter.sendMessage(failedMessage);
                return;
            }
            handlePutMessage(message);
        }
        else if (statusType == IKVMessage.StatusType.KEYRANGE) {
            handleKeyRange();
        }
        else if (statusType == IKVMessage.StatusType.KEYRANGE_READ) {
            handleKeyRangeRead();
        }
        else if (statusType == IKVMessage.StatusType.KEYS_TRANSFER) {
            handleKeyTransfer(message);
        }
        else if (statusType == IKVMessage.StatusType.RAFT_APPEND_ENTRIES) {
            kvServer.raftReceivedAppendEntries(message);
        }
        else if (statusType == IKVMessage.StatusType.RAFT_APPEND_ENTRIES_REPLY) {

        }
        else if (statusType == IKVMessage.StatusType.RAFT_REQUEST_VOTE) {
            kvServer.raftReceivedRequestVote(message);
        }
        else if (statusType == IKVMessage.StatusType.RAFT_REQUEST_VOTE_REPLY) {
            kvServer.receivedVoteReply(message);
        }
        else if (statusType == IKVMessage.StatusType.ECS_ADDRESS) {
            handleEcsAddress();
        }
        else {
            IKVMessage failedMessage = KVMessageFactory.createFailedMessage("Unknown Message");
            bufferWriter.sendMessage(failedMessage);
        }
    }

    private void handleEcsAddress() {
        String ecsConnectionString = kvServer.getCurrentEcsConnectionString();
        IKVMessage msg = KVMessageFactory.createEcsAddressSuccessMessage(ecsConnectionString);
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! Failed send message handleEcsAddress:" + e.getMessage());
        }
    }

    private boolean validateMessage(IKVMessage message){
        if (message.getKey().length() > Constants.KEY_MAX_LENGTH){
            return false;
        }
        if (message.getValue().length() > Constants.VALUE_MAX_LENGTH){
            return false;
        }
        return true;
    }

    private void handleGetMessage(IKVMessage message) throws IOException {
        String key = message.getKey();

        try
        {
            if (!isResponsibleGet(key)) {
                IKVMessage msg = KVMessageFactory.createServerNotResponsibleMessage(key + "|" + metadata.serialize());
                bufferWriter.sendMessage(msg);
                return;
            }
            if (!kvServer.inStorage(key))
                throw new Exception("key not found:" + key);
            String value = kvServer.getKV(key);
            IKVMessage returnMessage = KVMessageFactory.createGetSuccessMessage(key, value);
            bufferWriter.sendMessage(returnMessage);
        }
        catch (Exception e){
            IKVMessage returnMessage = KVMessageFactory.createGetErrorMessage(key);
            bufferWriter.sendMessage(returnMessage);
        }
    }
    private boolean isResponsible(String key) {
        String responsibleServer = metadata.getServerByKey(key);
        String responsibleServerPort = responsibleServer.split(":")[1];
        String myPort = kvServer.getHashRingIdentity().split(":")[1];
        //System.out.println("[DEBUG]ResponsibleServer:" + responsibleServer);
        //System.out.println("[DEBUG]MyIdentity:" + kvServer.getHashRingIdentity());
        return responsibleServerPort.equals(myPort);
    }
    private boolean isResponsibleGet(String key) {
        String responsibleServer = metadata.getServerByKey(key);
        ArrayList<String> replicas = metadata.getReplicas(responsibleServer);
        String responsibleServerPort = responsibleServer.split(":")[1];
        String myPort = kvServer.getHashRingIdentity().split(":")[1];
        if (!responsibleServerPort.equals(myPort)) {
            for (String r : replicas) {
                ServerInfo replica = new ServerInfo(r);
                if (myPort.equals(replica.getPort() + ""))
                    return true;
            }
        }
        //System.out.println("[DEBUG]ResponsibleServer:" + responsibleServer);
        //System.out.println("[DEBUG]MyIdentity:" + kvServer.getHashRingIdentity());
        return responsibleServerPort.equals(myPort);
    }
    private void handlePutMessage(IKVMessage message) throws IOException {
        String key = message.getKey();
        String value = message.getValue();
        try{
            if (kvServer.isServerLocked()) {
                IKVMessage lockMsg = KVMessageFactory.createServerWriteLockMessage();
                bufferWriter.sendMessage(lockMsg);
                return;
            }
            if (!isResponsible(key)) {
                IKVMessage msg = KVMessageFactory.createServerNotResponsibleMessage(key + "|" + metadata.serialize());
                bufferWriter.sendMessage(msg);
                return;
            }
            IKVStorage.PutOperationResult putResult = kvServer.putKV(key, value);

            IKVMessage returnMessage;
            if (putResult == IKVStorage.PutOperationResult.SUCCESS) {
                forwardToReplica(key, value);
                returnMessage = KVMessageFactory.createPutSuccessMessage(key, value);
            }
            else if (putResult == IKVStorage.PutOperationResult.DELETE) {
                forwardToReplica(key, value);
                returnMessage = KVMessageFactory.createDeleteSuccessMessage(key);
            }
            else {
                forwardToReplica(key, value);
                returnMessage = KVMessageFactory.createPutUpdateMessage(key, value);
            }

            bufferWriter.sendMessage(returnMessage);
        }
        catch (Exception e){
            logger.error(e.getMessage());
            IKVMessage returnMessage = KVMessageFactory.createPutErrorMessage(key);
            bufferWriter.sendMessage(returnMessage);
        }
    }

    private synchronized boolean forwardToReplica(String key, String value) {
        boolean success = true;
        KeyTransferComm keyComm = new KeyTransferComm();
        String transferKeyPayload = key + "," + value;

        String myConnectionString = kvServer.getHashRingIdentity();
        // Get Destination Replica
        ArrayList<String> replicaConnectionStringList = metadata.getReplicas(myConnectionString);
        for (String replicaConnString : replicaConnectionStringList) {
            ServerInfo replica = new ServerInfo(replicaConnString);
            logger.info("Forward to [" + replicaConnString + "]" + transferKeyPayload);
            try {
                keyComm.keyTransfer(replica.getAddress(), replica.getPort(), transferKeyPayload);
            }
            catch (IOException ex) {
                logger.error("forwardToReplica Error! [" + replica + "]" + transferKeyPayload + ex.getMessage());
                success = false;
            }
        }

        return success;
    }

    private void handleKeyRange() {
        String addr = kvServer.getHostname() + ":" + kvServer.getPort();
        IKVMessage msg = KVMessageFactory.createKeyRangeSuccessMessage(metadata.serialize());
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! Failed send message handleKeyRange:" + e.getMessage());
        }
    }

    private void handleKeyRangeRead() {
        String keyrange = metadata.serialize();
        String[] keyrangeEntries = keyrange.split(";");
        for (String keyrangeEntry : keyrangeEntries) {
            String[] fields = keyrangeEntry.split(",");
            if (fields.length >= 2) {
                String from = fields[0];
                String to = fields[1];
                String coordinatorConnStr = fields[2];
                ArrayList<String> replicaConnStringList = metadata.getReplicas(coordinatorConnStr);
                for (String replicaConnStr : replicaConnStringList) {
                    keyrange += from + "," + to + "," + replicaConnStr + ";";
                }
            }
        }
        IKVMessage msg = KVMessageFactory.createKeyRangeReadSuccessMessage(keyrange);
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! Failed send message handleKeyRange:" + e.getMessage());
        }
    }

    private void handleKeyTransfer(IKVMessage message) {
        logger.debug("handleKeyTransfer[" + this.kvServer.getHostname() + ":" + kvServer.getPort() + "]:"+ message.getValue());
        String keys = message.getValue();
        // <key1>,<value1>;<key2>..

        if (!"".equals(keys)) {
            String key;
            String value;
            String[] pairs = keys.split(";");
            try {
                for (String pair : pairs) {
                    key = pair.split(",")[0];
                    value = pair.split(",")[1];
                    kvServer.putKV(key, value);
                }
            } catch (Exception e) {
                logger.error("Error! Failed to handle key transfer:" + e.getMessage() + " keys:" + keys);
            }
        }
    }


}
