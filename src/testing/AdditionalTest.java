package testing;

import app_kvServer.KVServer;
import client.KVStore;
import org.junit.Test;

import junit.framework.TestCase;
import shared.KVMessageFactory;
import shared.ObjectFactory;
import shared.messages.IKVMessage;
import shared.messages.serialization.IKVMessageSerializer;
import shared.messages.serialization.KVMessagePlainTextSerializer;
import shared.services.KVService;
import testing.helper.KVBotTestingPlan;
import testing.helper.KVClientBot;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;

	public void setUp() {
		kvClient = new KVStore(ObjectFactory.createHashRing(),"localhost", 8182);
	}

	@Test
	public void testInsert() {
	}



	@Test
	public void testStub() {
		assertTrue(true);
	}

	@Test
	public void testSerializerGet() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createGetMessage("key");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerPut() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createPutMessage("key", "value");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey()) &&
					deserializedMessage.getValue().equals(message.getValue());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerGetSuccess() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createGetSuccessMessage("key", "value");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey()) &&
					deserializedMessage.getValue().equals(message.getValue());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerGetError() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createGetErrorMessage("key");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerPutSuccess() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createPutSuccessMessage("key", "value");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey()) &&
					deserializedMessage.getValue().equals(message.getValue());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerPutUpdate() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createPutUpdateMessage("key", "value");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey()) &&
					deserializedMessage.getValue().equals(message.getValue());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerDeleteSuccess() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createDeleteSuccessMessage("key");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerDeleteError() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createDeleteErrorMessage("key");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey());
		}
		catch (Exception e) { ex = e; }
		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerPutError() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createPutErrorMessage("key");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getKey().equals(message.getKey());
		}
		catch (Exception e) { ex = e; }

		assertTrue(ex == null && messageIdentical);
	}

	@Test
	public void testSerializerFailed() {
		Exception ex = null;
		boolean messageIdentical = false;
		try {
			IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
			IKVMessage message = KVMessageFactory.createFailedMessage("value");
			byte[] serializedMsg = serializer.serialize(message);
			IKVMessage deserializedMessage = serializer.deserialize(serializedMsg);
			messageIdentical = deserializedMessage.getStatus() == message.getStatus() &&
					deserializedMessage.getValue().equals(message.getValue());
		}
		catch (Exception e) { ex = e; }

		assertTrue(ex == null && messageIdentical);
	}


}
