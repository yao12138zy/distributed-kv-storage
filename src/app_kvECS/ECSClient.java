package app_kvECS;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import ecs.IECSNode;
import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;
import shared.ObjectFactory;
import shared.ServerStatus;
import shared.messages.serialization.KVMessagePlainTextSerializer;
import shared.services.KVService;

public class ECSClient implements IECSClient {
    private static final Logger logger = Logger.getRootLogger();


    private int port;
    private String hostname;
    private ServerSocket ecsSocket;
    boolean running = false;
    private Map<String, StorageServerHandler> kvNodes = new HashMap<>();
    private List<RequestQueueItem> requestQueue = new ArrayList<>();
    private int proposalNumber = 0;
    private EECSStatus ecsStatus = EECSStatus.READY;
    private IHashRing metadata;
    Lock kvNodesLock = new ReentrantLock();
    public ECSClient(String address, int port, IHashRing metadata) {
        this.metadata = metadata;
        this.hostname = address;
        this.port = port;
    }

    @Override
    public synchronized boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        for (String nodeName: nodeNames) {
            logger.info("Remove node:" +nodeName);
            kvNodes.remove(nodeName);
        }
        return false;
    }

    @Override
    public synchronized Map<String, IECSNode> getNodes() {
        Map<String, IECSNode> nodes = new HashMap<>();
        for(Map.Entry<String, StorageServerHandler> entry : kvNodes.entrySet()) {
            nodes.put(entry.getKey(), entry.getValue());
        }
        return nodes;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return kvNodes.get(Key);
    }

    private void setEcsStatus(EECSStatus status){
        // TODO: Thread safe
        ecsStatus = status;
    }
    public EECSStatus getEcsStatus() {
        return ecsStatus;
    }
    @Override
    public synchronized boolean joinNode(String hashIdentity, StorageServerHandler node) {
        if (metadata.serverExist(hashIdentity)) {
            logger.error("ECS:joinNode: Server already exist:" + hashIdentity);
            return false;
        }
        logger.trace("ECS:joinNode:" + hashIdentity);
        if (getEcsStatus() == EECSStatus.BUSY) {
            node.setNodeStatus(ServerStatus.QueuedJoin);
            queueNode(RequestQueueType.JOIN, node);
            return false;
        }
        else {
            node.setNodeStatus(ServerStatus.WriteLock);
        }
        setEcsStatus(EECSStatus.BUSY);
        proposalNumber += 1;



        metadata.addServer(hashIdentity);

        // This for loop includes the new joined node, cus RequestWriteLock will broadcase the new keyrange
        broadcastRequestWriteLock();
        return true;
    }

    @Override
    public synchronized boolean removeNode(String hashIdentity, StorageServerHandler node) {
        logger.trace("ECS:removeNode:" + hashIdentity);
        if (getEcsStatus() == EECSStatus.BUSY) {
            node.setNodeStatus(ServerStatus.QueuedShutdown);
            queueNode(RequestQueueType.SHUTDOWN, node);
            return false;
        }
        else {
            node.setNodeStatus(ServerStatus.WriteLock);
            node.setIsShuttingDown(true);
        }
        setEcsStatus(EECSStatus.BUSY);
        proposalNumber += 1;



        metadata.removeServer(hashIdentity);
        // This for loop includes the new joined node, cus RequestWriteLock will broadcase the new keyrange
        broadcastRequestWriteLock();
        return true;
    }

    private void broadcastRequestWriteLock() {
        for(Map.Entry<String, StorageServerHandler> entry : kvNodes.entrySet()) {
            StorageServerHandler n = entry.getValue();
            if (n.getNodeStatus() == ServerStatus.Ready || n.getNodeStatus() == ServerStatus.WriteLock)
                n.requestWriteLock(proposalNumber);
        }
    }

    @Override
    public synchronized void reportNodeTransferComplete(int proposalNumber) {
        if (proposalNumber < this.proposalNumber)
            // Outdated msg
            return;
        //Check if all complete
        boolean allCompleted = true;
        int i = 0;
        for(Map.Entry<String, StorageServerHandler> entry : kvNodes.entrySet()) {
            StorageServerHandler n = entry.getValue();
            i++;
            if ((n.getNodeStatus() == ServerStatus.WriteLock) && n.getTransferCompleteProposalNumber() < this.proposalNumber) {
                allCompleted = false;
                break;
            }
        }
        if (allCompleted) {
            List<String> expiredNodes = new ArrayList<>();
            for(Map.Entry<String, StorageServerHandler> entry : kvNodes.entrySet()) {
                StorageServerHandler n = entry.getValue();
                if (n.getNodeStatus() == ServerStatus.WriteLock) {
                    if (n.getIsShuttingDown()) {
                        n.requestReleaseShutdownLock();
                        expiredNodes.add(n.getNodeName());
                    }
                    else {
                        n.requestReleaseWriteLock(this.proposalNumber);
                    }
                    n.setNodeStatus(ServerStatus.Ready);
                }
            }
            for (String expiredKey : expiredNodes) {
                kvNodes.remove(expiredKey);
            }
            setEcsStatus(EECSStatus.READY);
            processQueuedRequests();
        }
    }

    private void processQueuedRequests() {
        // TODO: Thread safe
        if (requestQueue.size() == 0)
            return;

        for (RequestQueueItem item : requestQueue) {
            StorageServerHandler kvServerHandler = item.getStorageServerHandler();
            if (item.getRequestQueueType() == RequestQueueType.JOIN) {
                metadata.addServer(kvServerHandler.getHashIdentity());
            } else {
                // Leave
                metadata.removeServer(kvServerHandler.getHashIdentity());
                kvServerHandler.setIsShuttingDown(true);
            }
            kvServerHandler.setNodeStatus(ServerStatus.WriteLock);
        }
        requestQueue.clear();
        proposalNumber += 1;
        for(Map.Entry<String, StorageServerHandler> entry : kvNodes.entrySet()) {
            StorageServerHandler n = entry.getValue();
            if (n.getNodeStatus() == ServerStatus.Ready || n.getNodeStatus() == ServerStatus.WriteLock)
                n.requestWriteLock(proposalNumber);
        }
    }

    public void queueNode(RequestQueueType queueType, StorageServerHandler kvServerHandler) {
        RequestQueueItem requestQueueItem = new RequestQueueItem(queueType, kvServerHandler);
        requestQueue.add(requestQueueItem);
    }

    private boolean initializeECS() {
        logger.info("Initialize ECS ...");
        try {
            InetAddress addr = InetAddress.getByName(hostname);
            int backlog = 50;
            ecsSocket = new ServerSocket(port, backlog, addr);

            logger.info("Server listening on port: " +
                    ecsSocket.getLocalPort());
            return true;
        }
        catch (IOException e) {
            logger.error("ECS Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }
    private boolean isRunning() {
        return running;
    }
    @Override
    public void run() {

        running = initializeECS();

        if (ecsSocket != null) {
            while (isRunning()) {
                try {
                    Socket storageServerSock = ecsSocket.accept();
                    StorageServerHandler storageServerHandler = new StorageServerHandler(metadata, storageServerSock, this);
                    logger.info("New Connection:" + storageServerHandler.getNodeName());
                    //hashRing.addServer(storageServerHandler.getNodeName());



                    kvNodes.put(storageServerHandler.getNodeName(), storageServerHandler);
                    new Thread(storageServerHandler).start();

                    logger.info("Connected to "
                            + storageServerSock.getInetAddress().getHostName()
                            + " on port " +  storageServerSock.getPort());
                }
                catch (IOException e) {
                    //logger.error("Error! Unable to establish connection. \n", e);
                    logger.info("ECSClient: connection closed");
                }
            }

        }
    }

    private static void Configure(){
        KVService services = KVService.getInstance();
        services.setKvSerializer(new KVMessagePlainTextSerializer());
    }

    public static void main(String[] args) {
        // TODO


        Options options = new Options();
        options.addOption(new Option("p", true, "the port of the ECS server"));
        options.addOption(new Option("a", true, "address the ECS server should listen to"));
        options.addOption(new Option("ll", true, "log level"));
        options.addOption(new Option("h", false, "display help"));
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("ecs", options);
                return;
            }
            int port  = -1;
            String address = "127.0.0.1";

            if (cmd.hasOption("p")) {
                port = Integer.parseInt(cmd.getOptionValue("p"));
            }
            else {
                System.out.println("Error! You need to specify a port");
                return;
            }
            if (cmd.hasOption("a")) {
                address = cmd.getOptionValue("a");
            }
            String logfile = "logs/ecs.log";
            Level logLevel = Level.ALL;
            if (cmd.hasOption("ll")) {
                String logLevelArg = cmd.getOptionValue("ll");
                if ("FINEST".equals(logLevelArg))
                    logLevel = Level.ALL;
                else
                    logLevel = Level.toLevel(logLevelArg);
            }

            new LogSetup(logfile, logLevel);

            Configure();
            IHashRing hashRing = ObjectFactory.createHashRing();
            IECSClient ecs = ObjectFactory.createECSClient(address, port, hashRing);
            new Thread(ecs).start();

        }
        catch (ParseException e) {
            System.out.println("Unexpected parse exception:" + e.getMessage());
        }
        catch (IOException e) {
            System.out.println("Unexpected io exception:" + e.getMessage());
        }
    }


}
