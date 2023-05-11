package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.Constants;
import shared.KVMessageFactory;
import shared.ObjectFactory;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import shared.services.KVService;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class InteractionTest extends TestCase {

	private KVStore kvClient;

	public void setUp() {
		kvClient = new KVStore(ObjectFactory.createHashRing(), "localhost", 8182);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}


	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;
		IKVMessage m = null;
		try {
			m = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(m != null && m.getStatus() == StatusType.FAILED);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testConcurrentWrite() {
		Exception ex = null;
		try {
			for (int i = 0; i < 100; i++) {
				ConcurrentAccess testThread = new ConcurrentAccess(this.kvClient, Integer.toString(i));
				testThread.run();
			}
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);

	}

	@Test
	public void testGetUnsetValue() {
		String key = "an_unset_value";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		boolean success = ex == null && response.getStatus() == StatusType.GET_ERROR;
		if (!success) {
			if (ex != null) {
				System.out.println(ex.getMessage());
				ex.printStackTrace();
			}
			if (response != null)
				System.out.println("response:" + response);
		}
		assertTrue(success);
	}

	@Test
	public void testGetExtraLongKey() {
		String key = "k".repeat(Constants.KEY_MAX_LENGTH + 1);
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.get(key);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		boolean success = ex == null && response.getStatus().equals(StatusType.FAILED);
		if (!success) {
			if (ex != null) {
				System.out.println(ex.getMessage());
				ex.printStackTrace();
			}
			if (response != null)
				System.out.println("response:" + response);
		}
		assertTrue(success);
	}
	@Test
	public void testPutExtraLongKey() {
		String key = "k".repeat(Constants.KEY_MAX_LENGTH + 1);
		String value = "val";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus().equals(StatusType.FAILED));
	}

	@Test
	public void testPutExtraLongValue() {
		String key = "testPutExtraLongValue";
		String value = "v".repeat(Constants.VALUE_MAX_LENGTH + 1);
		IKVMessage response = null;
		Exception ex = null;

		try {

			response = kvClient.put(key, value);;
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus().equals(StatusType.FAILED));
	}

	@Test
	public void testPutLongKeyValue() {
		String key = "v".repeat(Constants.KEY_MAX_LENGTH);
		String value = "0123456789".repeat(Constants.VALUE_MAX_LENGTH / 10);
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);;
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus().equals(StatusType.PUT_SUCCESS));
	}

	@Test
	public void testGetLongKey() {
		String key = "k".repeat(Constants.KEY_MAX_LENGTH);
		String value = "v".repeat(Constants.VALUE_MAX_LENGTH);
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			Thread.sleep(2000);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		boolean success = ex == null &&
				response.getStatus().equals(StatusType.GET_SUCCESS)
				&& "v".repeat(Constants.VALUE_MAX_LENGTH).equals(response.getValue());
		if (!success) {
			if (ex != null) {
				System.out.println(ex.getMessage());
				ex.printStackTrace();
			}
			if (response != null) {
				System.out.println("response:" + response);
			}
			if (!"v".repeat(Constants.VALUE_MAX_LENGTH).equals(response.getValue())){
				System.out.println("response not equal to expected value");
			}
		}
		assertTrue(success);
	}
}
