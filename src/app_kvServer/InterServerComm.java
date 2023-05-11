package app_kvServer;

import org.apache.log4j.Logger;
import shared.KVMessageFactory;
import shared.messages.IKVMessage;
import shared.messages.buffer.IKVMessageBufferWriter;
import shared.messages.buffer.KVMessageCrLfBufferWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class InterServerComm extends Thread {
    private Logger logger = Logger.getRootLogger();
    private boolean running;

    private String address;
    private int port;
    private Socket connectionSocket;
    private OutputStream output;
    private InputStream input;
    private IKVMessageBufferWriter bufferWriter;

    public InterServerComm(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public void sendAndDisconnect(IKVMessage message) throws IOException {
        connectionSocket = new Socket(address, port);
        output = connectionSocket.getOutputStream();
        input = connectionSocket.getInputStream();
        bufferWriter = new KVMessageCrLfBufferWriter(output);
        sendMessage(message);
        closeConnection();

    }

    private void sendMessage(IKVMessage message) {
        try {
            bufferWriter.sendMessage(message);
        } catch (IOException e) {
            logger.error("Failed to send key transfer message");
            e.printStackTrace();
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
