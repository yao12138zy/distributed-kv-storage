package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import app_kvServer.cache.IKVCache;
import app_kvServer.cache.LRUCache;
import client.IKVSocketListener;
import client.KVStore;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import shared.Constants;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;
import shared.ObjectFactory;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.serialization.KVMMessageUTFSerializer;
import shared.messages.serialization.KVMessagePlainTextSerializer;
import shared.services.KVService;


import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

public class KVClient extends Thread implements IKVClient, IKVSocketListener {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private boolean stop = false;
    private KVCommInterface client = null;
    private IHashRing metadata;
    public KVClient() {
        metadata = ObjectFactory.createHashRing();
    }
    @Override
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException{
        metadata.getHashRingMap().clear();
        KVStore kvStore = new KVStore(metadata, hostname, port);
        kvStore.addListener(this);
        kvStore.connect();
        this.client = kvStore;
    }


    @Override
    public KVCommInterface getStore(){
        return client;
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine)
    {
        String[] tokens = cmdLine.split("\\s+");

        String command = tokens[0];

        if (command.equals("connect"))
        {
            handleCommandConnect(tokens);
        }
        else if (command.equals("disconnect"))
        {
            handleCommandDisconnect(tokens);
        }
        else if (command.equals("put"))
        {
            handleCommandPut(tokens);
        }
        else if (command.equals("get"))
        {
            handleCommandGet(tokens);
        }
        else if (command.equals("logLevel"))
        {
            handleCommandLogLevel(tokens);
        }
        else if (command.equals("help"))
        {
            handleCommandHelp();
        }
        else if (command.equals("quit"))
        {
            handleCommandQuit();
        }
        else
        {
            printError("Unknown command");
            printHelp();
        }
    }
    private void handleCommandConnect(String[] tokens)
    {
        if (tokens.length == 3)
        {
            try
            {
                String argServerAddress = tokens[1];
                int argServerPort = Integer.parseInt(tokens[2]);
                newConnection(argServerAddress, argServerPort);
                printStdOut("Connection established ...");
            }
            catch (NumberFormatException nfe)
            {
                printError("No valid address. Port must be a number!");
                logger.info("Unable to parse argument <port>", nfe);
            }
            catch (UnknownHostException e)
            {
                printError("Unknown Host!");
                logger.info("Unknown Host!", e);
            }
            catch (IOException e)
            {
                printError("Could not establish connection!");
                logger.warn("Could not establish connection!", e);
            }
        }
        else
        {
            printInvalidNumberOfParams();
        }
    }
    private void disconnect(){
        if (client != null) {
            client.disconnect();
            client = null;
        }
    }
    private void handleCommandDisconnect(String[] tokens)
    {
        //printStdOut("[NOT IMPLEMENTED][disconnect]");
        disconnect();
        printStdOut("Disconnected from Server...");
    }
    private void handleCommandPut(String[] tokens)
    {
        if (tokens.length >= 3)
        {
            StringBuilder sbValue = new StringBuilder();
            for(int i = 2; i < tokens.length; i++) {
                sbValue.append(tokens[i]);
                if (i != tokens.length -1 ) {
                    sbValue.append(" ");
                }
            }
            String key = tokens[1];
            String value = sbValue.toString();

            /** Length check on key and value */
            if (key.getBytes().length > Constants.KEY_MAX_LENGTH) {
                logger.error("Input key length exceeds 20 bytes");
                printError("Input key length exceeds 20 bytes");
                return;
            }
            if (value.getBytes().length > Constants.VALUE_MAX_LENGTH) {
                logger.error("Input value length exceeds 120 kilobytes");
                printError("Input value length exceeds 120 kilobytes");
                return;
            }

            //printStdOut("[NOT IMPLEMENTED][put] key:" + key + " Value:" + value);
            try{
                IKVMessage response = client.put(key, value);
                /*
                printStdOut("Server Response:(" +
                        response.getStatus().toString() + ")[" +
                        response.getKey() + "][" +
                        response.getValue() + "]");
                 */
                if (response.getStatus() == IKVMessage.StatusType.PUT_SUCCESS) {
                    printStdOut("key-value pair was inserted successfully.");
                } else if (response.getStatus() == IKVMessage.StatusType.DELETE_SUCCESS) {
                    printStdOut(key + " was successfully deleted from storage.");
                } else if (response.getStatus() == IKVMessage.StatusType.PUT_UPDATE) {
                    printStdOut(String.format("key \"%s\" was successfully updated with value \"%s\".",key,value));
                } else {
                    logger.debug("PUT command failed:" + response);
                    printError("PUT command failed.");
                }
            }
            catch (InterruptedException ex){
                logger.error("Put Channel Jammed");
                printError("Put Channel Jammed");
            }
            catch (IOException ex){
                logger.error("Connection error: GET");
                printError("Connection error");
            }
        }
        else
        {
            printInvalidNumberOfParams();
        }
    }
    private void handleCommandGet(String[] tokens)
    {
        if (tokens.length == 2)
        {
            String key = tokens[1];
            //printStdOut("[NOT IMPLEMENTED][get] key:" + key);
            try{
                IKVMessage response = client.get(key);
                /*  printStdOut("Server Response:(" +
                        response.getStatus().toString() + ")[" +
                        response.getKey() + "][" +
                        response.getValue() + "]");
                 */
                if (response.getStatus() == IKVMessage.StatusType.GET_SUCCESS) {
                    printStdOut("Value retrieved: " + response.getValue());
                } else {
                    String details = "";
                    if (response.getStatus() == IKVMessage.StatusType.FAILED){
                        details = ":" + response.getValue();
                    }
                    printError("GET command failed" + details);
                }

            }
            catch (InterruptedException ex){
                logger.error("Get Channel Jammed");
                printError("Get Channel Jammed");
            }
            catch (IOException ex){
                logger.error("Connection error: GET");
                printError("Connection error");
            }
        }
        else
        {
            printInvalidNumberOfParams();
        }
    }
    private void handleCommandLogLevel(String[] tokens)
    {
        if (tokens.length == 2)
        {
            String level = setLevel(tokens[1]);
            if (level.equals(LogSetup.UNKNOWN_LEVEL))
            {
                printError("No valid log level!");
                printPossibleLogLevels();
            }
            else
            {
                printStdOut(PROMPT + "Log level changed to level" + level);
            }
        }
        else
        {
            printInvalidNumberOfParams();
        }
    }

    private void handleCommandHelp()
    {
        printHelp();
    }
    private void handleCommandQuit()
    {
        stop = true;
        disconnect();
        printStdOut(PROMPT + "Application exit!");
    }
    private void printHelp()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <text message>");
        sb.append("\t\t puts a key value pair into the storage server. \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the storage server. \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("help");
        sb.append("\t\t\t prints help information \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }
    private void printInvalidNumberOfParams()
    {
        printError("Invalid number of parameters!");
    }

    private void printPossibleLogLevels()
    {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }
    private void printError(String msg)
    {
        System.out.println("[ERROR] " + msg);
    }
    private void printStdOut(String msg)
    {
        System.out.println("[INFO] "+ msg);
    }

    private String setLevel(String levelString)
    {
        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    public static void main(String[] args)
    {
        try
        {
            new LogSetup("logs/client.log", Level.OFF);

            KVService services = KVService.getInstance();
            services.setKvSerializer(new KVMessagePlainTextSerializer());


            KVClient app = new KVClient();
            app.run();
        }
        catch (IOException e)
        {

        }
    }

    @Override
    public void handleNewMessage(IKVMessage msg) {

    }

    @Override
    public void handleStatus(SocketStatus status) {

    }
}
