package app_kvECS;


import ecs.IECSNode;
import org.apache.log4j.Logger;
import shared.Hash.IHashRing;
import shared.KVMessageFactory;
import shared.ServerStatus;
import shared.messages.IKVMessage;
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
import java.util.List;

public class StorageServerHandler implements Runnable, IECSNode {
    private static final Logger logger = Logger.getRootLogger();
    private final Socket storageServerSocket;
    private final IECSClient ecs;
    private InputStream input;
    private OutputStream output;
    private IKVMessageBufferWriter bufferWriter;
    private boolean isOpen;
    private boolean isSafeShutdown;
    private boolean isShutttingDown = false;
    private ServerStatus serverStatus;
    private String hashIdentity = "";
    private int transferCompleteProposalNumber = -1;
    private IHashRing metadata;
    public StorageServerHandler(IHashRing hashRing, Socket socket, IECSClient ecs) {
        this.metadata = hashRing;
        storageServerSocket = socket;
        this.ecs = ecs;
        isOpen = true;
        serverStatus = ServerStatus.Unknown;
    }

    @Override
    public void run() {
        try {
            output = storageServerSocket.getOutputStream();
            input = storageServerSocket.getInputStream();
            bufferWriter = new KVMessageCrLfBufferWriter(output);
            IKVMessageBufferReader bufferReader = new KVMessageCrLfBufferReader(storageServerSocket, input);

            while (isOpen) {
                try {
                    IKVMessage msg = bufferReader.readNextMessage();
                    messageHandler(msg);
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                }
                catch (IOException ioe) {
                    logger.info("Storage Server Connection lost!");
                    isOpen = false;
                }
            }

        }
        catch (IOException e) {
            logger.error("Error! Connection could not be established!", e);
        }
        finally {
            try {
                List<String> nodeList = new ArrayList<>();
                nodeList.add(this.getNodeName());
                ecs.removeNodes(nodeList);
                if (!isSafeShutdown) {
                    logger.info("[Unsafe Shutdown Detected]Initialte reconciliation:" + getNodeName() + "," + hashIdentity);
                    ecs.removeNode(hashIdentity, this);
                }
                if (storageServerSocket != null) {
                    input.close();
                    output.close();
                    storageServerSocket.close();
                }

            }
            catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }

    }

    public int getTransferCompleteProposalNumber() {
        // TODO: Need to be thread safe
        return transferCompleteProposalNumber;
    }

    private void messageHandler(IKVMessage message) throws IOException {
        // Await status
        //
        IKVMessage.StatusType status = message.getStatus();
        if (status == IKVMessage.StatusType.JOIN_ECS) {
            handleJoinECS(message);
        }
        else if (status == IKVMessage.StatusType.REPORT_TRANSFER_COMPLETE) {
            handleTransferComplete(message);
        }
        else if (status == IKVMessage.StatusType.LEAVE_ECS) {
            handleLeaveECS();
        }
    }
    private void handleLeaveECS() {
        // Hash Identity is something like: 192.0.0.1:8080
        ecs.removeNode(hashIdentity, this);

    }
    private void handleJoinECS(IKVMessage msg){
        String hashIdentity = msg.getKey();

        // Hash Identity is something like: 192.0.0.1:8080
        this.hashIdentity = hashIdentity;
        ecs.joinNode(hashIdentity, this);
    }

    private void handleTransferComplete(IKVMessage msg) {
        // TODO: Thread safe required
        int transferCompleteProposalNumber = -1;
        String rawProposalNumber = msg.getKey();
        try {
            transferCompleteProposalNumber = Integer.parseInt(rawProposalNumber);
            this.transferCompleteProposalNumber = transferCompleteProposalNumber;
            ecs.reportNodeTransferComplete(transferCompleteProposalNumber);
        }
        catch (NumberFormatException e) {
            logger.error("handleTransferComplete, invalid proposal number:" + rawProposalNumber);
        }

    }

    public String getHashIdentity() {
        return hashIdentity;
    }


    public void requestWriteLock(int proposalNumber) {
        String keyrange = metadata.serialize();
        IKVMessage msg = KVMessageFactory.createRequestWriteLockMessage(proposalNumber, keyrange);
        try {
            serverStatus = ServerStatus.WriteLock;
            bufferWriter.sendMessage(msg);
            transferCompleteProposalNumber = -1;
        }
        catch (IOException e) {
            logger.error("Error! RequestWriteLock:" + e.getMessage());
        }
    }


    public void requestReleaseWriteLock(int proposalNumber) {
        IKVMessage msg = KVMessageFactory.createRequestReleaseWriteLockMessage(proposalNumber);
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! RequestReleaseWriteLock:" + e.getMessage());
        }
    }

    public void requestReleaseShutdownLock() {
        IKVMessage msg = KVMessageFactory.createRequestReleaseShutdownLockMessage();
        isSafeShutdown = true;
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! RequestReleaseWriteLock:" + e.getMessage());
        }
    }

    @Override
    public String getNodeName() {
        /*
        System.out.println("getInetAddress-getHostName:" + storageServerSocket.getInetAddress().getHostName());
        System.out.println("getInetAddress-getHostAddress:" + storageServerSocket.getInetAddress().getHostAddress());


        System.out.println("getInetAddress-getCanonicalHostName:" + storageServerSocket.getInetAddress().getCanonicalHostName());
        System.out.println("getLocalAddress-getHostName:" + storageServerSocket.getLocalAddress().getHostName());
        System.out.println("getLocalAddress-getHostAddress:" + storageServerSocket.getLocalAddress().getHostAddress());
        System.out.println("getLocalAddress-getCanonicalHostName:" + storageServerSocket.getLocalAddress().getCanonicalHostName());
        System.out.println("getRemoteSocketAddress:" + storageServerSocket.getRemoteSocketAddress());

         */
        String host = storageServerSocket.getInetAddress().getHostName();
        int port = storageServerSocket.getPort();
        return host + ":" + port;
    }

    @Override
    public String getNodeHost() {
        return storageServerSocket.getInetAddress().getHostName();
    }

    @Override
    public int getNodePort() {
        return storageServerSocket.getPort();
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[] { "0000", "FFFF" };
    }

    @Override
    public ServerStatus getNodeStatus() {
        return serverStatus;
    }
    @Override
    public void setNodeStatus(ServerStatus status) {
        this.serverStatus = status;
    }

    @Override
    public boolean getIsShuttingDown() {
        return this.isShutttingDown;
    }

    @Override
    public void setIsShuttingDown(boolean isShuttingDown) {
        this.isShutttingDown = isShuttingDown;
    }
}
