package app_kvServer;

import org.apache.log4j.Logger;
import shared.KVMessageFactory;
import shared.messages.IKVMessage;
import shared.messages.buffer.IKVMessageBufferReader;
import shared.messages.buffer.IKVMessageBufferWriter;
import shared.messages.buffer.KVMessageCrLfBufferReader;
import shared.messages.buffer.KVMessageCrLfBufferWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class BootstrapServerComm extends Thread {
    private Logger logger = Logger.getRootLogger();
    private boolean running;

    private String address;
    private int port;
    private Socket connectionSocket;
    private OutputStream output;
    private InputStream input;
    private IKVMessageBufferWriter bufferWriter;

    private KVServer kvServer;
    public BootstrapServerComm(String address, int port, KVServer kvServer) {
        this.address = address;
        this.port = port;
        this.kvServer = kvServer;
    }


    public void connect() throws IOException {
        System.out.println("Connecting to bootstrap server: " + address + ":" + port);
        connectionSocket = new Socket(address, port);
        output = connectionSocket.getOutputStream();
        input = connectionSocket.getInputStream();
        bufferWriter = new KVMessageCrLfBufferWriter(output);
        running = true;
        start();
    }
    public void sendEcsAddressRequest() {
        IKVMessage message = KVMessageFactory.createEcsAddressMessage();
        sendMessage(message);
    }
    private void sendMessage(IKVMessage message) {
        try {
            bufferWriter.sendMessage(message);
        } catch (IOException e) {
            logger.error("Failed to send key transfer message");
            e.printStackTrace();
        }
    }
    private void handleMessgae(IKVMessage message) {
        if (message.getStatus() == IKVMessage.StatusType.ECS_ADDRESS_SUCCESS) {
            kvServer.notifyEcsFound(message.getKey());
            closeConnection();
        }
    }

    public void run() {
        try {
            IKVMessageBufferReader bufferReader = new KVMessageCrLfBufferReader(connectionSocket, input);
            while (running) {
                try {
                    IKVMessage latestMsg = bufferReader.readNextMessage();
                    handleMessgae(latestMsg);
                }
                /*
                catch (InterruptedException ex) {
                    logger.error("Channel Jammed!");
                }

                 */
                catch (IOException ioe) {
                    if (running) {
                        kvServer.notifyEcsDisconnected();
                        try {
                            tearDownConnection();
                        } catch (IOException e) {
                            logger.error("Unable to close ECS connection!");
                        }
                    }
                }
            }
        } finally {
            if (running) {
                closeConnection();
            }
        }
    }

    public void close() {
        running = false;
        if (connectionSocket != null && !connectionSocket.isClosed()) {
            try {
                connectionSocket.close();
            }
            catch (IOException e){
                //logger.error("Failed Close Key Transfer Connection Socket:" + e.getMessage());
            }
        }
    }

    public synchronized void closeConnection() {
        //logger.info("try to close Key Transfer connection ...");
        try {
            tearDownConnection();
        } catch (IOException ioe) {
            //logger.error("Unable to close Key Transfer connection!");
        }
    }

    private void tearDownConnection() throws IOException {
        this.running = false;
        //logger.info("tearing down the Key Transfer connection ...");
        if (connectionSocket != null) {
            connectionSocket.close();
            connectionSocket = null;
            //logger.info("Key Transfer connection closed!");
        }
    }


}
