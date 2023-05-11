package app_kvServer;

import app_kvServer.IKVServer;
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
import java.net.StandardSocketOptions;
import java.util.concurrent.Semaphore;

public class ECSConnection extends Thread {
    private Logger logger = Logger.getRootLogger();

    private String address;
    private int port;
    private boolean running;
    private boolean closing = false;
    private Socket connectionSocket;
    private OutputStream output;
    private InputStream input;
    private IKVMessageBufferWriter bufferWriter;
    private IKVServer kvServer;
    private Semaphore semConWaitShutdown = new Semaphore(0);
    private IHashRing metadata;
    public ECSConnection(String address, int port, IKVServer kvServer, String hashIdentity) {
        this.metadata = kvServer.getMetadata();
        this.address = address;
        this.port = port;
        this.kvServer = kvServer;
        //kvServer.start();
    }
    public void run() {
        try {
            System.out.println("ECSConnection: run()");


            IKVMessageBufferReader bufferReader = new KVMessageCrLfBufferReader(connectionSocket, input);
            while (isRunning()) {
                try {
                    IKVMessage latestMsg = bufferReader.readNextMessage();
                    handleMessgae(latestMsg);
                    /*
                    for (IKVSocketListener listener : listeners) {
                        listener.handleNewMessage(latestMsg);
                    }

                     */
                }
                /*
                catch (InterruptedException ex) {
                    logger.error("Channel Jammed!");
                }

                 */
                catch (IOException ioe) {
                    if (isRunning()) {
                        kvServer.notifyEcsDisconnected();
                        logger.error("ECS Connection lost!");
                        try {
                            tearDownConnection();
                            /*
                            for (IKVSocketListener listener : listeners) {
                                listener.handleStatus(
                                        IKVSocketListener.SocketStatus.CONNECTION_LOST);
                            }

                             */
                        } catch (IOException e) {
                            logger.error("Unable to close ECS connection!");
                        }
                    }
                }
            }
        } finally {
            if (isRunning()) {
                closeConnection();
            }
        }
    }

    public void connect() throws IOException {
        // TODO Auto-generated method stub
        connectionSocket = new Socket(address, port);
        output = connectionSocket.getOutputStream();
        input = connectionSocket.getInputStream();
        bufferWriter = new KVMessageCrLfBufferWriter(output);
        setRunning(true);
        logger.info("ECS Connection established");
        SendJoinECSMessage(kvServer.getHashRingIdentity());
        start();
    }

    private void SendJoinECSMessage(String hashIdentity) {
        IKVMessage msg = KVMessageFactory.createJoinECSMessage(hashIdentity);
        try {
            bufferWriter.sendMessage(msg);
        } catch (IOException e) {
            logger.error("Error! SendJoinECSMessage:" + e.getMessage(), e);
        }
    }
    public void close(boolean kill) {
        if (isRunning()) {
            if (!kill)
                requestClose();
            else {
                running = false;
                closeConnection();
            }
        }
        else {
            System.out.println("ECSConnection: close() - already closed");
        }
    }
    private void requestClose() {
        logger.info("ECS Shutting down...");

        setClosing(true);
        sendLeaveECS();
        try {
            semConWaitShutdown.acquire();
        }
        catch (InterruptedException e) {
            logger.error("Shutdown error, ECS sync interrupted...");
        }
        running = false;
        commitClose();
        logger.info("Shutting down complete...");
    }
    private void commitClose() {
        if (connectionSocket != null && !connectionSocket.isClosed()) {
            closeConnection();
        }
        kvServer.close();
    }

    public synchronized void closeConnection() {
        logger.info("try to close ECS connection ...");

        try {
            tearDownConnection();
            /*
            for(IKVSocketListener listener : listeners) {
                listener.handleStatus(IKVSocketListener.SocketStatus.DISCONNECTED);
            }
             */
        } catch (IOException ioe) {
            logger.error("Unable to close ECS connection!");
        }
    }

    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the ECS connection ...");
        if (connectionSocket != null) {
            //input.close();
            //output.close();
            connectionSocket.close();
            connectionSocket = null;
            logger.info("ECS connection closed!");
        }
    }

    private void setRunning(boolean run) {
        this.running = run;
    }
    private boolean isRunning() {
        return running;
    }

    private void handleMessgae(IKVMessage msg) {
        IKVMessage.StatusType status = msg.getStatus();
        if (status == IKVMessage.StatusType.REQUEST_WRITE_LOCK) {
            handleRequestWriteLock(msg);
        }
        else if (status == IKVMessage.StatusType.REQUEST_RELEASE_WRITE_LOCK) {
            handleRequestReleaseWriteLock(msg);
        }
        else if (status == IKVMessage.StatusType.REQUEST_RELEASE_SHUTDOWN_LOCK) {
            System.out.println("Release shutdown lock");
            handleRequestReleaseShutdownLock();
        }
        else {
            handleUnknownMessage(msg);
        }
    }


    private void handleRequestWriteLock(IKVMessage msg) {
        logger.info("handleRequestWriteLock Proposal Number:" + msg.getKey());
        logger.info("handleRequestWriteLock Key Range:" + msg.getValue());
        String rawProposalNumber = msg.getKey();
        try {
            int proposalNumber = Integer.parseInt(rawProposalNumber);
            String newKeyRange = msg.getValue().trim();
            if ("".equals(newKeyRange)) {
                //System.out.println("[Debug] Keyrange empty skip");
            }
            else {
                String oldMetadata = metadata.serialize();
                metadata.deserialize(newKeyRange);
                //System.out.println("[Debug] Synced keyrange");
                //System.out.println("[Debug] New Key Range:" + newKeyRange);
                if (!oldMetadata.equals(""))
                    kvServer.keyTransfer(oldMetadata, msg.getValue());
            }
            // Debug directly report transfer complete
            //try {
                //Thread.sleep(5000);
            //}
            //catch (InterruptedException e) {}
            sendTransferComplete(proposalNumber);
        }
        catch (NumberFormatException e) {
            logger.error("handleRequestWriteLock, invalid proposal number:" + rawProposalNumber);
        }

    }

    private void sendLeaveECS() {
        IKVMessage msg = KVMessageFactory.createLeaveECSMessage();
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! ESCConnection failed sendLeaveECS", e);
        }
    }

    public void sendTransferComplete(int proposalNumber) {
        IKVMessage msg = KVMessageFactory.createReportTransferCompleteMessage(proposalNumber);
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! ESCConnection failed SendReportServerStatusMessage", e);
        }
    }

    private void handleRequestReleaseWriteLock(IKVMessage msg) {
        String rawProposalNumber = msg.getKey();
        try {
                kvServer.unlockServer();
        }
        catch (NumberFormatException e) {
            logger.error("handleRequestReleaseWriteLock, invalid proposal number:" + rawProposalNumber, e);
        }

    }

    private void handleRequestReleaseShutdownLock() {
        kvServer.close();
        semConWaitShutdown.release();
    }

    private void sendFailedMessage(String description) {
        IKVMessage msg = KVMessageFactory.createFailedMessage(description);
        try {
            bufferWriter.sendMessage(msg);
        }
        catch (IOException e) {
            logger.error("Error! ESCConnection failed SendFailedMessage", e);
        }
    }
    private void handleUnknownMessage(IKVMessage msg) {
        logger.error("Error! ECSConnection encounters UnknownMessage");
    }


    private void setClosing(boolean closing) {
        this.closing = closing;
    }

    private boolean getClosing() {
        return this.closing;
    }

}
