package app_kvServer;

import app_kvECS.IECSClient;
import app_kvServer.cache.IKVCache;
import app_kvServer.consensus.raft.RaftMetadata;
import app_kvServer.storage.IKVStorage;
import app_kvServer.storage.KVFileStorage;
import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
//import org.apache.commons.io.IOUtils;
import shared.*;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;
import shared.Hash.KeyTransferInfo;
import shared.messages.IKVMessage;
import shared.messages.serialization.KVMessagePlainTextSerializer;
import shared.services.KVService;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import app_kvServer.consensus.raft.ERaftRole;


import logger.LogSetup;
import org.apache.log4j.Level;

import javax.xml.bind.DatatypeConverter;

public class KVServer extends Thread implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 *
	 *
	 * ******************** HANDLE CLIENT REQUESTS ON EACH CONNECTION.
	 */

	private static Logger logger = Logger.getRootLogger();
	private int port;
	private String hostname;
	private ServerSocket serverSocket;
	private boolean running = false;
	private final IKVStorage kvStorage;
	private final IKVCache kvCache;
	private Lock ECSWriteLock = new ReentrantLock();
	private boolean isLocked;
	private ReadWriteLock cacheReadWriteLock = new ReentrantReadWriteLock();
	private Lock cacheWriteLock = cacheReadWriteLock.writeLock();
	private Lock cacheReadLock = cacheReadWriteLock.readLock();
	private KeyTransferComm keyComm = new KeyTransferComm();
	private RaftMetadata raftMetadata;
	private boolean isEcsDisconnected = false;
	private IHashRing metadata;
	private ConsensusHandler consensusHandler;
	private String currentLeaderAddress = "";
	public KVServer(int port, String hostname, int cacheSize, String strategy, IKVStorage storage, String leaderConnctionString) {
		this.metadata = ObjectFactory.createHashRing();
		this.port = port;
		this.hostname = hostname;

		this.kvStorage = storage;
		this.kvCache = ObjectFactory.createKVCache(strategy, cacheSize);


		ServerInfo leaderInfo = new ServerInfo(leaderConnctionString);
		boolean isLeader = "".equals(leaderInfo.getAddress()); // if leaderAddress is empty, then this server is the leader
		System.out.println("isLeader:" + isLeader);
		if (isLeader) {
			raftMetadata = new RaftMetadata(metadata, ERaftRole.Leader);
			start();
		}
		else {
			raftMetadata = new RaftMetadata(metadata, ERaftRole.Follower);
			startBootstrapEcs(leaderConnctionString);
		}

	}

	private void connectToLeaderECS(String ecsConnectionString) {
		ServerInfo ecsInfo = new ServerInfo(ecsConnectionString);
		System.out.println("Connecting to leader ECS:" + ecsConnectionString);

		//ecs = ObjectFactory.createECSClient(ecsInfo.getAddress(), ecsInfo.getPort(), metadata);
		new Thread(ecs).start();
		try {
			Thread.sleep(1000);
			String hashRingIdentity = hostname + ":" + port;
			ecsConnection2 = new ECSConnection(ecsInfo.getAddress(), ecsInfo.getPort(), this, hashRingIdentity);
			ecsConnection2.connect();

		}
		catch (InterruptedException e) {
			System.out.println("Error! connectToLeaderECS: Interrupted");
		}
		catch (IOException e) {
			System.out.println("Error! connectToLeaderECS: cannot connect to ECS " + ecsInfo.getPort() + e.getMessage());
		}
	}

	private void startBootstrapEcs(String leaderConnectionString) {
		ServerInfo leaderInfo = new ServerInfo(leaderConnectionString);
		BootstrapServerComm bootstrapServerComm = new BootstrapServerComm(leaderInfo.getAddress(), leaderInfo.getPort(), this);

		try {
			bootstrapServerComm.connect();
			Thread.sleep(1000);
		}
		catch (IOException e) {
			System.out.println("Error! startBootstrapEcs: cannot connect to leader " + leaderInfo.getPort() + e.getMessage());
		}
		catch (InterruptedException e) {
			System.out.println("Error! startBootstrapEcs: Interrupted");
		}
		bootstrapServerComm.sendEcsAddressRequest();
	}

	public void notifyEcsFound(String ecsConnectionString) {
		System.out.println("ecsFound->" + ecsConnectionString);
		connectToLeaderECS(ecsConnectionString);
		start();
	}

	public void shuttingDown() {
		System.out.println("Shutting down kvServer...");
		if (ecsConnection2 != null)
			ecsConnection2.close(false);
	}

	private int getEcsPort() {
		return port + 100;
	}
	public String getCurrentEcsConnectionString() {
		return currentLeaderAddress;
	}
	public void notifyEcsDisconnected() {
		isEcsDisconnected = true;
	}
	static ECSConnection ecsConnection2;
	static IECSClient ecs;
	public void notifyElectedAsLeader(){
		int ecsPort = getEcsPort();
		ecs = ObjectFactory.createECSClient(hostname, ecsPort, metadata);
		currentLeaderAddress = hostname + ":" + ecsPort;
		new Thread(ecs).start();
		try {
			Thread.sleep(1000);
			String hashRingIdentity = hostname + ":" + port;
			ecsConnection2 = new ECSConnection("127.0.0.1", ecsPort, this, hashRingIdentity);
			System.out.println("One server is connected to ECS...");
			ecsConnection2.connect();

		}
		catch (InterruptedException e) {
			System.out.println("Error! notifyElectedAsLeader: Interrupted");
		}
		catch (IOException e) {
			System.out.println("Error! notifyElectedAsLeader: cannot start ECS " +ecsPort + e.getMessage());
		}
		sendAppendEntries();
	}
	public void notifyEcsClose() {
		if (ecsConnection2 != null) {
			ecsConnection2.close(false);
			System.out.println("" + ecs.toString());
		}
	}

	public void sendRequestVote(int termNumber) {
		RaftPayload payload = createRaftPayload(termNumber);
		IKVMessage requestVote = KVMessageFactory.createRaftRequestVoteMessage(payload);
		System.out.println("sendRequestVote");
		broadcastMessage(requestVote);
	}

	public void sendRequestVoteReply(int termNumber, boolean voteGranted, String serverAddress) {
		RaftPayload payload = createRaftPayload(termNumber);
		IKVMessage requestVoteReply = KVMessageFactory.createRaftRequestVoteReplyMessage(payload, voteGranted);
		try {
			ServerInfo server = new ServerInfo(serverAddress);
			InterServerComm comm = new InterServerComm(server.getAddress(), server.getPort());
			comm.sendAndDisconnect(requestVoteReply);
		} catch (IOException e) {
			logger.error("sendRequestVoteReply Error! Unable to establish connection. [" + serverAddress + "]\n", e);
		}
	}

	public void sendAppendEntries() {
		int termNumber = raftMetadata.getTerm();
		RaftPayload payload = createRaftPayload(termNumber);
		IKVMessage appendEntries = KVMessageFactory.createRaftAppendEntriesMessage(payload, hostname + ":" + (port + 100));
		broadcastMessage(appendEntries);
	}

	public void sendAppendEntriesReply(int termNumber, boolean success, String serverAddress) {
		RaftPayload payload = createRaftPayload(termNumber);
		IKVMessage appendEntriesReply = KVMessageFactory.createRaftAppendEntriesReplyMessage(payload, success);
		try {
			ServerInfo server = new ServerInfo(serverAddress);
			InterServerComm comm = new InterServerComm(server.getAddress(), server.getPort());
			comm.sendAndDisconnect(appendEntriesReply);
		} catch (IOException e) {
			logger.error("sendAppendEntriesReply Error! Unable to establish connection. [" + serverAddress + "]\n", e);
		}
	}


	public void receivedVoteReply(IKVMessage message) {
		RaftPayload receivedPayload = new RaftPayload(message.getKey());
		boolean voteGranted = Boolean.parseBoolean(message.getValue());

		if (voteGranted)
			consensusHandler.leaderVoteReceived(receivedPayload.getTermNumber());

	}

	public void raftReceivedRequestVote(IKVMessage message) {

		RaftPayload receivedPayload = new RaftPayload(message.getKey());
		String requesterAddress = receivedPayload.getAddress();
		ERaftRole role = raftMetadata.getRole();
		boolean grantVote = consensusHandler.receivedRequestVote(receivedPayload.getTermNumber());
		System.out.println("raftReceivedRequestVote:" + requesterAddress + "->" + grantVote);
		sendRequestVoteReply(raftMetadata.getTerm(), grantVote, requesterAddress);
	}

	public void raftReceivedAppendEntries(IKVMessage message) {
		RaftPayload receivedPayload = new RaftPayload(message.getKey());
		String requesterAddress = receivedPayload.getAddress();
		ERaftRole role = raftMetadata.getRole();
		consensusHandler.receivedAppendEntries(receivedPayload.getTermNumber());
		sendAppendEntriesReply(raftMetadata.getTerm(), true, requesterAddress);
		String newEcsAddress = message.getValue();
		currentLeaderAddress = newEcsAddress;
		if (isEcsDisconnected) {


			ServerInfo newEcsServer = new ServerInfo(newEcsAddress);
			System.out.println("Re-establishing connection to ECS:" + newEcsAddress);
			try {
				String hashRingIdentity = hostname + ":" + port;
				ecsConnection2 = new ECSConnection(newEcsServer.getAddress(), newEcsServer.getPort(), this, hashRingIdentity);
				ecsConnection2.connect();
				isEcsDisconnected = false;
			}
			catch (IOException e) {
				System.out.println("Error! raftReceivedAppendEntries: cannot connect to ECS ");
			}
		}
	}

	private RaftPayload createRaftPayload(int termNumber) {
		RaftPayload payload = new RaftPayload(termNumber, hostname + ":" + port);
		return payload;
	}

	private ArrayList<ServerInfo> getOtherClusterServers() {
		ArrayList<ServerInfo> servers = new ArrayList<ServerInfo>();
		String serverListRaw = "";
		if (metadata != null)
			serverListRaw = metadata.serialize();
		String[] serverList = serverListRaw.split(";");
		for (String server : serverList) {
			String[] serverInfo = server.split(",");
			if (serverInfo.length == 3) {
				ServerInfo info = new ServerInfo(serverInfo[2]);
				if (!info.getAddress().equals(this.hostname) || info.getPort() != this.port)
					servers.add(info);
			}
		}
		return servers;
	}

	private void broadcastMessage(IKVMessage message){
		ArrayList<ServerInfo> servers = getOtherClusterServers();
		System.out.println("Broadcasting to " + servers.size() + " servers:" + message.getStatus());
		for (ServerInfo server : servers) {
			System.out.println("Recepient: " + server.getAddress() + ":" + server.getPort());
			try {
				InterServerComm comm = new InterServerComm(server.getAddress(), server.getPort());
				comm.sendAndDisconnect(message);
			} catch (IOException e) {
				logger.error("broadcastMessage Error! Unable to establish connection. [" + server.getAddress() + ":" + server.getPort() + "]\n", e);
			}
		}
	}

	@Override
	public void start() {
		consensusHandler = new ConsensusHandler(raftMetadata, this);
		new Thread(consensusHandler).start();

		running = initializeServer();
		super.start();
	}

	public void run() {

		if (serverSocket != null) {
			while(isRunning()) {
				try {
					Socket client = serverSocket.accept();
					KVClientHandler clientHandler = new KVClientHandler(metadata, client, this);
					new Thread(clientHandler).start();
					/*
					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());

					 */
				}
				catch (IOException e) {
					//logger.error("Error! " +
					//		"Unable to establish connection. [" + this.port + "]\n", e);
					logger.info("KVServer: connection closed");
				}
			}
		}
	}

	@Override
	public String getHashRingIdentity() {
		return hostname + ":" + port;
	}



	@Override
	public boolean isServerLocked() {
		boolean locked;
		try {
			this.ECSWriteLock.lock();
			locked = this.isLocked;
		} finally {
			this.ECSWriteLock.unlock();
		}
		return locked;
	}

	@Override
	public void unlockServer() {
		try {
			this.ECSWriteLock.lock();
			this.isLocked = false;
		} finally {
			this.ECSWriteLock.unlock();
		}
	}

	@Override
	public void keyTransfer(String oldMeta, String newMeta) {
		try {
			this.ECSWriteLock.lock();
			this.isLocked = true;
		} finally {
			this.ECSWriteLock.unlock();
		}
		try {
			IHashRing oldMetadata = new HashRing();
			oldMetadata.deserialize(oldMeta);
			ArrayList<KeyTransferInfo> info_list = oldMetadata.syncData(newMeta);
			for (KeyTransferInfo info : info_list) {
				System.out.println("syncData info: " + info.toString());
			}
			String own = String.format("%s:%d",this.hostname,this.port);

			for (KeyTransferInfo info : info_list) {
				String resultKeys = "";
				if (own.equals(info.sender) && !info.sender.equals(info.receiver)) {
					ServerInfo destServer = new ServerInfo(info.receiver);
					Set<String> keys = kvStorage.getKeys();
					for (String key: keys) {
						if (oldMetadata.keyInRange(key,info.range_start, info.range_end)) {
							resultKeys = resultKeys + String.format("%s,%s;",key,kvStorage.getKV(key));
						}
					}
					keyComm.keyTransfer(destServer.getAddress(),destServer.getPort(),resultKeys);
				}
			}
			// update local hashRing
			metadata.deserialize(newMeta);
			ArrayList<String> deletedKeys = new ArrayList<String>();
			ArrayList<byte[]> newRange = metadata.syncRange(own);

			for (byte[] r : newRange) {
				System.out.println("syncrange : " + DatatypeConverter.printHexBinary(r));
			}
			//System.out.println(UnsignedBytes.lexicographicalComparator().compare(newRange.get(0),newRange.get(1)));
			if (!newRange.isEmpty() && (UnsignedBytes.lexicographicalComparator().compare(newRange.get(0),newRange.get(1)) != 0)) {
				Set<String> keys = kvStorage.getKeys();
				for (String key: keys) {
					if (!metadata.keyInRange(key,newRange.get(0), newRange.get(1))) {
						System.out.println("deleted key: " + key);
						deletedKeys.add(key);
					}
				}
				kvStorage.deleteKeys(deletedKeys);
			}

		} catch	(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public IHashRing getMetadata() {
		return this.metadata;
	}

	private boolean isRunning() {
		return this.running;
	}

	private boolean initializeServer(){
		logger.info("Initialize server ...");
		try {
			InetAddress addr = InetAddress.getByName(hostname);
			int backlog = 50;
			serverSocket = new ServerSocket(port, backlog, addr);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;
		}
		catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	@Override
	public int getPort(){
		return this.port;
	}

	@Override
	public String getHostname(){
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
	public CacheStrategy getCacheStrategy(){
		return kvCache.getCacheStrategy();
	}

	@Override
	public int getCacheSize(){
		return kvCache.getCacheSize();
	}

	@Override
	public boolean inStorage(String key){
		return kvStorage.inStorage(key);
	}

	@Override
	public boolean inCache(String key){
		try {
			cacheReadLock.lock();
			return kvCache.inCache(key);
		}
		finally {
			cacheReadLock.unlock();
		}

	}


	/** Implement centralized data storage by Paul
	 *
	 *
	 */
	@Override
	public String getKV(String key) throws Exception {
		String cacheVal = null;
		try {
			cacheReadLock.lock();
			cacheVal = kvCache.get(key);
		}
		finally {
			cacheReadLock.unlock();
		}
		if (cacheVal != null) {
			//System.out.println("getting value from cache");
			return cacheVal;
		} else {
			//System.out.println("getting value from disk");
			String fetchVal =  kvStorage.getKV(key);
			try {
				cacheWriteLock.lock();
				kvCache.put(key,fetchVal);
			}
			finally {
				cacheWriteLock.unlock();
			}
			return fetchVal;
		}
	}

	@Override
	public IKVStorage.PutOperationResult putKV(String key, String value) throws Exception {
		if ("null".equals(value))
			value = null;
		if ("\"\"".equals(value))
			value = null;
		String cacheVal = null;
		try {
			cacheReadLock.lock();
			cacheVal = kvCache.get(key);
		}
		finally {
			cacheReadLock.unlock();
		}
		if (cacheVal != null && cacheVal.equals(value))
			return IKVStorage.PutOperationResult.SUCCESS;
		IKVStorage.PutOperationResult result = kvStorage.putKV(key, value);
		if (result != IKVStorage.PutOperationResult.ERROR) {
			//System.out.println("updating in cache");
			try {
				cacheWriteLock.lock();
				kvCache.put(key, value);
			}
			finally {
				cacheWriteLock.unlock();
			}
		}
		return result;
	}
	@Override
	public void clearCache(){
		try {
			cacheWriteLock.lock();
			kvCache.clearCache();
		}
		finally {
			cacheWriteLock.unlock();
		}
	}

	@Override
	public void clearStorage() {
		kvStorage.clearStorage();
	}




	@Override
	public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
	public void close(){
		logger.info("The server is shutting down...");
		running = false;
		if (serverSocket != null && !serverSocket.isClosed()) {
			try {
				serverSocket.close();
				serverSocket = null;
			}
			catch (IOException e){
				logger.error("Failed Close Server Socket:" + e.getMessage());
			}
		}
	}

	public static void main(String[] args) {
		try {
			Options options = new Options();
			options.addOption(new Option("p", true, "the port of the server"));
			options.addOption(new Option("a", true, "address the server should listen to"));
			options.addOption(new Option("b", true, "address and port of the ecs server address:port"));
			options.addOption(new Option("d", true, "directory for files"));
			options.addOption(new Option("s", true, "cache strategy"));
			options.addOption(new Option("c", true, "cache size"));
			options.addOption(new Option("l", true, "relative path of the logfile"));
			options.addOption(new Option("ll", true, "loglevel"));
			options.addOption(new Option("h", false, "display help"));
			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);
			if (cmd.hasOption("h")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("m1-server", options);
				return;
			}
			int port  = -1;
			String address = "127.0.0.1";
			String dirForFiles = "";
			String logfile = "logs/server.log";
			Level logLevel = Level.ALL;
			String cacheStrategy = "";
			int cacheSize = 10;
			String ecsAddress = "";
			Integer ecsPort = 0;
			if (cmd.hasOption("s")) {
				cacheStrategy = cmd.getOptionValue("s");
			}
			if (cmd.hasOption("c")) {
				try {
					cacheSize = Integer.parseInt(cmd.getOptionValue("c"));
				}
				catch (NumberFormatException e) {
					System.out.println("Error! You need to specify a valid cache size");
					return;
				}
			}
			if (cmd.hasOption("p")) {
				port = Integer.parseInt(cmd.getOptionValue("p"));
			}
			else {
				System.out.println("Error! You need to specify a port");
				return;
			}
			if (cmd.hasOption("b")) {
				String[] ecsArgs = cmd.getOptionValue("b").split(":");
				if (ecsArgs.length == 2) {
					ecsAddress = ecsArgs[0];
					try {
						ecsPort = Integer.parseInt(ecsArgs[1]);
					}
					catch (NumberFormatException e) {
						System.out.println("Error! You need to specify a valid ecs server port");
						return;
					}
				}
				else {
					ecsAddress = "";
					System.out.println("No ECS provided, starting in standalone mode");
					//return;
				}
			}
			else {
				ecsAddress = "";
				System.out.println("No ECS provided, starting in standalone mode");
			}
			if (cmd.hasOption("a")) {
				address = cmd.getOptionValue("a");
			}
			if (cmd.hasOption("d")) {
				dirForFiles = cmd.getOptionValue("d");
			}
			if (cmd.hasOption("l")) {
				logfile = cmd.getOptionValue("l");
			}
			if (cmd.hasOption("ll")) {
				logLevel = Level.toLevel(cmd.getOptionValue("ll"), Level.ALL);
			}

			new LogSetup(logfile, logLevel);

			System.out.println("Server started[Address]" + address);
			System.out.println("Server started[port]" + port);

			KVService services = KVService.getInstance();
			services.setKvSerializer(new KVMessagePlainTextSerializer());
			Path dataFilePath = Paths.get(dirForFiles, "storage.dat");
			// use a memory storage
			//services.setKvStorage(new KVMemoryStorage());
			if (!Files.exists(Paths.get(dirForFiles))) {
				System.out.println("Data dir does not exist, create:" + dirForFiles);
				Files.createDirectory(Paths.get(dirForFiles));
			}
			IKVStorage storage = new KVFileStorage(dataFilePath.toString());
			final IKVServer kvServer = ObjectFactory.createKVServerObject(port, address, cacheSize, cacheStrategy, storage, ecsAddress + ":" + ecsPort);

			Runtime.getRuntime().addShutdownHook(new Thread()
			{
				public void run()
				{
					kvServer.shuttingDown();

				}
			});




		}
		catch (InvalidPathException e) {
			System.out.println("Error! Invalid path");
		}
		catch (NumberFormatException e) {
			System.out.println("Error! Invalid port number");
		}
		catch (ParseException e) {
			System.out.println("Unexpected exception:" + e.getMessage());
		}
		catch (IOException e) {
			System.out.println("Error! Unexpected Error:" + e.getMessage());
		}
	}
}
