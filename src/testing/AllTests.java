package testing;

import java.io.IOException;

import app_kvECS.IECSClient;
import app_kvServer.ECSConnection;
import app_kvServer.IKVServer;
import app_kvServer.storage.IKVStorage;
import app_kvServer.storage.KVFileStorage;
import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import shared.ObjectFactory;
import shared.messages.serialization.KVMessagePlainTextSerializer;
import shared.services.KVService;


public class AllTests {


	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			KVService services = KVService.getInstance();
			services.setKvSerializer(new KVMessagePlainTextSerializer());
			/*
			IECSClient ecs = ObjectFactory.createECSClient("localhost", 8181);
			new Thread(ecs).start();

			IKVStorage storage1 = new KVFileStorage("test_kv1.dat");
			storage1.clearStorage();
			IKVServer server1 = (ObjectFactory.createKVServerObject(8182, "localhost", 10, "FIFO", storage1));
			final ECSConnection ecsConn1 = new ECSConnection("localhost", 8181, server1, "localhost:8182");
			ecsConn1.connect();

			IKVStorage storage2 = new KVFileStorage("test_kv2.dat");
			storage2.clearStorage();
			IKVServer server2 = (ObjectFactory.createKVServerObject(8183, "localhost", 10, "FIFO", storage2));
			final ECSConnection ecsConn2 = new ECSConnection("localhost", 8181, server2, "localhost:8183");
			ecsConn2.connect();

			IKVStorage storage3 = new KVFileStorage("test_kv3.dat");
			storage3.clearStorage();
			IKVServer server3 = (ObjectFactory.createKVServerObject(8184, "localhost", 10, "FIFO", storage3));
			final ECSConnection ecsConn3 = new ECSConnection("localhost", 8181, server3, "localhost:8184");
			ecsConn3.connect();
			*/
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		//clientSuite.addTestSuite(ConnectionTest.class);
		//clientSuite.addTestSuite(InteractionTest.class);
		//clientSuite.addTestSuite(AdditionalTest.class);
		//clientSuite.addTestSuite(HashRingTest.class);
		//clientSuite.addTestSuite(PerformanceTest.class);
		//clientSuite.addTestSuite(ECSTest.class);
		clientSuite.addTestSuite(RaftTest.class);
		return clientSuite;
	}


	
}
