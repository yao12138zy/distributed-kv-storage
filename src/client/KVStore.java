package client;

import jdk.jshell.spi.ExecutionControl;
import shared.KVMessageFactory;
import shared.ServerInfo;
import shared.messages.IKVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;

import org.apache.log4j.Logger;
import shared.messages.buffer.IKVMessageBufferReader;
import shared.messages.buffer.IKVMessageBufferWriter;
import shared.messages.buffer.KVMessageCrLfBufferReader;
import shared.messages.buffer.KVMessageCrLfBufferWriter;
import shared.Hash.IHashRing;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 *
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	private final int MAX_RETRY = 3;
	private Logger logger = Logger.getRootLogger();
	private Set<IKVSocketListener> listeners;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private IKVMessageBufferWriter bufferWriter;



	private String address;
	private int port;
	private boolean running;
	KVSocketResponseListener putListener;
	KVSocketResponseListener getListener;
	//M2
	KVSocketResponseListener keyRangeListener;
	//M3
	KVSocketResponseListener keyRangeReadListener;

	private IHashRing metaData;
	private String metaDataRead;

	public KVStore(IHashRing metadata, String address, int port) {
		this.metaData = metadata;
		this.address = address;
		this.port = port;

		listeners = new HashSet<IKVSocketListener>();

		Set<IKVMessage.StatusType> putWaitStatus = EnumSet.of(IKVMessage.StatusType.PUT_ERROR,
				IKVMessage.StatusType.PUT_SUCCESS,
				IKVMessage.StatusType.PUT_UPDATE,
				IKVMessage.StatusType.DELETE_SUCCESS,
				IKVMessage.StatusType.DELETE_ERROR,
				IKVMessage.StatusType.FAILED,
				//M2
				IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE,
				IKVMessage.StatusType.SERVER_WRITE_LOCK);
		putListener = new KVSocketResponseListener(putWaitStatus);
		listeners.add(putListener);

		Set<IKVMessage.StatusType> getWaitStatus = EnumSet.of(IKVMessage.StatusType.GET_ERROR,
				IKVMessage.StatusType.GET_SUCCESS,
				IKVMessage.StatusType.FAILED,
				//M2
				IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
		getListener = new KVSocketResponseListener(getWaitStatus);
		listeners.add(getListener);

		//M2
		Set<IKVMessage.StatusType> keyRangeWaitStatus = EnumSet.of(IKVMessage.StatusType.KEYRANGE_SUCCESS,
				IKVMessage.StatusType.FAILED);
		keyRangeListener = new KVSocketResponseListener(keyRangeWaitStatus);
		listeners.add(keyRangeListener);

		//M3
		Set<IKVMessage.StatusType> keyRangeReadWaitStatus = EnumSet.of(IKVMessage.StatusType.KEYRANGE_READ_SUCCESS,
				IKVMessage.StatusType.FAILED);
		keyRangeReadListener = new KVSocketResponseListener(keyRangeReadWaitStatus);
		listeners.add(keyRangeReadListener);
	}

	private void setRunning(boolean run) {
		this.running = run;
	}

	private boolean isRunning() {
		return running;
	}

	public void runEx() {
		try {

			IKVMessageBufferReader bufferReader = new KVMessageCrLfBufferReader(clientSocket, input);

			while (isRunning()) {
				try {
					IKVMessage latestMsg = bufferReader.readNextMessage();
					logger.debug("listener.handleNewMessage:" + latestMsg);
					for (IKVSocketListener listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (InterruptedException ex) {
					logger.error("Channel Jammed!");
				} catch (IOException ioe) {
					if (isRunning()) {

						logger.info("Connection lost!");
						try {
							tearDownConnection();
							for (IKVSocketListener listener : listeners) {
								listener.handleStatus(
										IKVSocketListener.SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
						metaData.removeServer(address + ":" + port);
					}
				}
			}
		} finally {
			if (isRunning()) {
				closeConnection();
			}
		}
	}

	@Override
	public void connect() throws IOException {
		clientSocket = new Socket(address, port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		bufferWriter = new KVMessageCrLfBufferWriter(output);
		setRunning(true);
		logger.info("Connection established");
		new Thread() {
			public void run() {
				runEx();
			}

		}.start();
	}

	public synchronized void closeConnection() {
		logger.info("try to close connection1 ...");

		try {
			tearDownConnection();
			for (IKVSocketListener listener : listeners) {
				listener.handleStatus(IKVSocketListener.SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");

		try {
			metaData.getHashRingMap().clear();
			address = "";
			port = 0;
			tearDownConnection();
			for (IKVSocketListener listener : listeners) {
				listener.handleStatus(IKVSocketListener.SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	public void addListener(IKVSocketListener listener) {
		listeners.add(listener);
	}

	@Override
	public IKVMessage put(String key, String value) throws IOException, InterruptedException {
		if (key.contains(" ")) {
			logger.error("KVStore.get:Illigal key [" + key + "]");
			return KVMessageFactory.createFailedMessage("Illigal key:" + key);
		}
		switchToResponsibleServer(key, "PUT");
		if (!running)
			return KVMessageFactory.createFailedMessage("Service is not reachable. Please reconnect.");
		IKVMessage msg = KVMessageFactory.createPutMessage(key, value);
		bufferWriter.sendMessage(msg);
		IKVMessage responseMsg = putListener.getMsg();

		int retry = 0;
		while (responseMsg.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			if (retry >= MAX_RETRY) {
				logger.error("put error! max retry reached. SERVER_NOT_RESPONSIBLE");
				responseMsg = KVMessageFactory.createFailedMessage("Internal service error. Please try again.");
				break;
			}
			if (updateMetadata("PUT")) {
				switchToResponsibleServer(key, "PUT");
			}
			else {
				logger.error("put error! Service is not reachable.");
				responseMsg = KVMessageFactory.createFailedMessage("Service is not reachable. Please reconnect.");
				return responseMsg;
			}
			bufferWriter.sendMessage(msg);
			if (!running)
				return KVMessageFactory.createFailedMessage("Service is not reachable. Please reconnect.");
			responseMsg  = putListener.getMsg();
			retry++;
		}

		return responseMsg;
	}

	@Override
	public IKVMessage get(String key) throws IOException, InterruptedException {
		if (key.contains(" ")) {
			logger.error("KVStore.get:Illigal key [" + key + "]");
			return KVMessageFactory.createFailedMessage("Illigal key:" + key);
		}
		switchToResponsibleServer(key, "GET");
		if (!running)
			return KVMessageFactory.createFailedMessage("Service is not reachable. Please reconnect.");
		IKVMessage msg = KVMessageFactory.createGetMessage(key);
		bufferWriter.sendMessage(msg);
		IKVMessage responseMsg = getListener.getMsg();

		int retry = 0;
		while (responseMsg.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			if (retry >= MAX_RETRY) {
				logger.error("get error! max retry reached. SERVER_NOT_RESPONSIBLE key:" + responseMsg.getKey());
				responseMsg = KVMessageFactory.createFailedMessage("Internal service error. Please try again.");
				break;
			}
			if (updateMetadata("GET")) {
				switchToResponsibleServer(key, "GET");
			}
			else {
				logger.error("get error! Service is not reachable.");
				responseMsg = KVMessageFactory.createFailedMessage("Service is not reachable. Please reconnect.");
				return responseMsg;
			}
			bufferWriter.sendMessage(msg);
			if (!running)
				return KVMessageFactory.createFailedMessage("Service is not reachable. Please reconnect.");
			responseMsg  = getListener.getMsg();
			retry++;
		}


		return responseMsg;
	}

	//M2
	public IKVMessage keyrange() throws IOException, InterruptedException {
		IKVMessage msg = KVMessageFactory.createKeyRangeMessage();
		bufferWriter.sendMessage(msg);
		IKVMessage responseMsg = keyRangeListener.getMsg();
		return responseMsg;
	}

	//M3
	public IKVMessage keyrange_read() throws IOException, InterruptedException {
		IKVMessage msg = KVMessageFactory.createKeyRangeReadMessage();
		bufferWriter.sendMessage(msg);
		IKVMessage responseMsg = keyRangeReadListener.getMsg();
		return responseMsg;
	}


	private List<ServerInfo> getAllAvailableServerFromMetadata() {
		String hashringText = metaData.serialize();
		List<ServerInfo> serverList = ServerInfo.hashRingToServerInfoList(hashringText);
		return serverList;
	}
	private void removeCurrentServerFromServerList(List<ServerInfo> serverList) {
		for (int i = 0; i < serverList.size(); i++) {
			if (serverList.get(i).getAddress().equals(address) && serverList.get(i).getPort() == port) {
				serverList.remove(i);
				break;
			}
		}
	}
	private boolean updateMetadata(String actionName) {
		List<ServerInfo> serverList = getAllAvailableServerFromMetadata();
		removeCurrentServerFromServerList(serverList);
		// Our current connection is the default server to ask for keyrange
		if (!"".equals(address) && port != 0)
			serverList.add(0, new ServerInfo(address, port));
		for (int i = 0 ; i < serverList.size(); i++) {
			// Ask each server in our keyrange for keyrange
			ServerInfo server = serverList.get(i);
			try {
				logger.debug("updateMetadata get from server [" + i + "]:" + server.getConnectionString());
				switchToServer(server);

				logger.trace("updateMetadata():" + actionName);
				if (actionName.equals("PUT")) {
					IKVMessage keyrangeResponse = keyrange();
					if (keyrangeResponse.getStatus() == IKVMessage.StatusType.KEYRANGE_SUCCESS) {
						String keyrangeRaw = keyrangeResponse.getKey();
						logger.debug("getMetadata()-KeyrangeRaw:" + keyrangeRaw);
						try {
							metaData.deserialize(keyrangeRaw);
						}
						catch (ArrayIndexOutOfBoundsException e) {
							System.out.println("ArrayIndexOutOfBoundsException:" + keyrangeRaw);
							throw e;
						}
						return true;

					} else {
						logger.error("updateMetadata failed![" + i + "/" + serverList.size() + "] [" + keyrangeResponse.getStatus() + "] Messgae:" + keyrangeResponse.getKey() + keyrangeResponse.getValue());
					}
				}
				else if (actionName.equals("GET")) {
					IKVMessage keyrangeResponse = keyrange_read();
					if (keyrangeResponse.getStatus() == IKVMessage.StatusType.KEYRANGE_READ_SUCCESS) {
						String keyrangeReadRaw = keyrangeResponse.getKey();
						logger.debug("getMetadata()-KeyrangeReadRaw:" + keyrangeReadRaw);
						try {
							metaDataRead = keyrangeReadRaw;
						}
						catch (ArrayIndexOutOfBoundsException e) {
							System.out.println("ArrayIndexOutOfBoundsException:" + keyrangeReadRaw);
							throw e;
						}
						return true;

					} else {
						logger.error("updateMetadata failed![" + i + "/" + serverList.size() + "] [" + keyrangeResponse.getStatus() + "] Messgae:" + keyrangeResponse.getKey() + keyrangeResponse.getValue());
					}
				}
				else {
					logger.error("updateMetadata failed! Invalid actionName:" + actionName);
				}

			} catch (IOException e) {
				logger.debug("updateMetadata failed![" + i + "/" + serverList.size() + "] [" + server.getConnectionString() + "] IOException:" + e.getMessage(), e);
			} catch (InterruptedException e) {
				logger.error("updateMetadata failed! Timeout.", e);
			}
		}
		return false;
	}

	private IHashRing getMetadata(String actionName) {
		if (actionName.equals("PUT")) {
			if (metaData.isEmpty()) {
				// The first this is called, issue KEYRANGE and init keyring
				if (!updateMetadata("PUT")) {
					logger.debug("getMetadata error! Service is not reachable");
				}
			}
		}
		else if (actionName.equals("GET")) {
			if (metaDataRead == null || metaDataRead.trim().equals("")) {
				// The first this is called, issue KEYRANGE and init keyring
				if (!updateMetadata("GET")) {
					logger.debug("getMetadata error! Service is not reachable");
				}
			}
		}
		return metaData;
	}

	private ServerInfo getResponsibleServer(String key, String actionName) {
		IHashRing hashRing = getMetadata(actionName);
		if (actionName.equals("PUT")) {
			if (hashRing.isEmpty()) {
				return new ServerInfo("");
			}
			String connectionString = hashRing.getServerByKey(key);
			ServerInfo serverInfo = new ServerInfo(connectionString);
			return serverInfo;
		}
		else if (actionName.equals("GET")) {
			if (hashRing.isEmpty()) {
				return new ServerInfo("");
			}
			String connectionString = hashRing.getServerByKey(key);
			if (metaDataRead == null || metaDataRead.equals(""))
				return new ServerInfo("");
			String[] metadataEntries = metaDataRead.split(";");
			List<ServerInfo> metadataServerInfos = ServerInfo.hashRingToServerInfoList(metaDataRead);
			ArrayList<ServerInfo> candidates = new ArrayList<>();
			String start = "", end = "";
			if (metadataServerInfos.size() == 1) {
				return metadataServerInfos.get(0);
			}
			for (int i = 0; i < metadataEntries.length; i++) {
				if (metadataServerInfos.get(i).getConnectionString().equals(connectionString)) {
					String entry = metadataEntries[i];
					String[] fields = entry.split(",");
					start = fields[0];
					end = fields[1];
					break;
				}
			}
			for (int i = 0; i < metadataEntries.length; i++) {
				String entry = metadataEntries[i];
				String[] fields = entry.split(",");
				if (start.equals(fields[0]) && end.equals(fields[1])) {
					candidates.add(metadataServerInfos.get(i));
				}
			}

			int randomIndex = (int)(Math.random() * candidates.size());
			if (candidates.size() == 0) {
				System.out.println("[Debug]Empty candidates:" + metaDataRead + "|" + key + "|" + metaData.serialize() + "|" + connectionString);
				return new ServerInfo("");
			}
			//System.out.println("GET Candidates:" + candidates.size() + " Index:" + randomIndex);
			return candidates.get(randomIndex);
		}
		else {
			logger.error("getResponsibleServer error! Invalid action name:" + actionName);
			return new ServerInfo("");
		}
	}
	private void switchToResponsibleServer(String key, String actionName) {

		int retry = 0;
		while (retry < MAX_RETRY) {
			ServerInfo serverInfo = getResponsibleServer(key, actionName);
			try {
				logger.info("switchToResponsibleServer: server[" + serverInfo.getConnectionString() + "] ");
				switchToServer(serverInfo);
				return;
			} catch (IOException e) {
				logger.info("switchToResponsibleServer: server[" + serverInfo.getConnectionString() + "] is not available. Initiate update metadata.");
				if (!updateMetadata(actionName)) {
					logger.error("switchToResponsibleServer error! cannot update metadata");
					break;
				}
			}
			retry++;
		}
		logger.error("switchToResponsibleServer error! metadata invalid");
	}
	private void switchToServer(ServerInfo server) throws IOException {
		logger.trace("switchToServer:" + server.getConnectionString());
		if (server.getIsValid()) {
			if (!server.getAddress().equals(this.address) || server.getPort() != port || !isRunning()) {
				logger.trace("switchToServer: switching");
				logger.debug("switchToServer:" + server.getConnectionString());
				this.address = server.getAddress();
				this.port = server.getPort();
				tearDownConnection();
				connect();
			}
			else {
				logger.trace("switchToServer: no need to switch");
			}
		}
		else {
			logger.trace("switchToServer: invalid server info");
		}
	}


}