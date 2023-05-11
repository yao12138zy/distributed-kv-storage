package app_kvServer;

import org.apache.log4j.Logger;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;
import shared.KVMessageFactory;
import shared.messages.IKVMessage;
import shared.messages.buffer.IKVMessageBufferWriter;
import shared.messages.buffer.KVMessageCrLfBufferWriter;
import shared.services.KVService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class KeyTransferComm extends Thread {

    private Logger logger = Logger.getRootLogger();
    private boolean running;

    private String address;
    private int port;
    private Socket connectionSocket;

    private String keyPairs;
    private OutputStream output;
    private InputStream input;
    private IKVMessageBufferWriter bufferWriter;

    public KeyTransferComm() {
    }

    public void keyTransfer(String address, int port, String pairs) throws IOException {
        connectionSocket = new Socket(address, port);
        this.keyPairs = pairs;
        output = connectionSocket.getOutputStream();
        input = connectionSocket.getInputStream();
        bufferWriter = new KVMessageCrLfBufferWriter(output);
        sendKeys();
        logger.info("Key Transfer success");
        closeConnection();

    }

    private void sendKeys() {
        IKVMessage msg = KVMessageFactory.createKeyTransferMessage(keyPairs);
        try {
            bufferWriter.sendMessage(msg);
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
                logger.error("Failed Close Key Transfer Connection Socket:" + e.getMessage());
            }
        }
    }

    public synchronized void closeConnection() {
        logger.info("try to close Key Transfer connection ...");
        try {
            tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close Key Transfer connection!");
        }
    }

    private void tearDownConnection() throws IOException {
        this.running = false;
        logger.info("tearing down the Key Transfer connection ...");
        if (connectionSocket != null) {
            connectionSocket.close();
            connectionSocket = null;
            logger.info("Key Transfer connection closed!");
        }
    }




}
