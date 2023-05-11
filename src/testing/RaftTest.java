package testing;
import app_kvServer.consensus.raft.ERaftRole;
import app_kvServer.consensus.raft.RaftCoutdown;
import app_kvServer.consensus.raft.RaftMetadata;
import app_kvServer.consensus.raft.RaftTimeout;
import junit.framework.TestCase;
import org.junit.Test;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;
import shared.KVMessageFactory;
import shared.RaftPayload;
import shared.messages.IKVMessage;
import shared.services.KVService;

import java.security.NoSuchAlgorithmException;

public class RaftTest extends TestCase  {

    @Test
    public void testRaftPayload() {

        Exception ex = null;
        boolean test1 = false, test2 = false;
        try {
            RaftPayload payload = new RaftPayload(98, "127.0.0.1:9092");
            test1 = payload.getTermNumber() == 98 && payload.getAddress().equals("127.0.0.1:9092");
            String serializedPayload = payload.serialize();
            RaftPayload deserializedPayload = new RaftPayload(serializedPayload);
            test2 = deserializedPayload.getTermNumber() == 98 && deserializedPayload.getAddress().equals("127.0.0.1:9092");
        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);

        }
        assertTrue(success);
    }

    @Test
    public void testRaftRequestVoteSerialization() {

        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false;
        try {
            RaftPayload payload = new RaftPayload(99, "127.0.0.1:9091");
            IKVMessage raftRequestVoteMsg = KVMessageFactory.createRaftRequestVoteMessage(payload);
            byte[] buffer = KVService.getInstance().getKvMessageSerializer().serialize(raftRequestVoteMsg);
            IKVMessage deserializedMsg = KVService.getInstance().getKvMessageSerializer().deserialize(buffer);

            test1 = deserializedMsg.getStatus() == IKVMessage.StatusType.RAFT_REQUEST_VOTE;
            RaftPayload deserializedPayload = new RaftPayload(deserializedMsg.getKey());
            test2 = deserializedPayload.getTermNumber() == 99;
            test3 = deserializedPayload.getAddress().equals("127.0.0.1:9091");
        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2
                && test3;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);
            System.out.println("test3:" + test3);

        }
        assertTrue(success);
    }


    @Test
    public void testRaftRequestVoteReplySerialization() {

        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false, test4 = false;
        try {
            RaftPayload payload = new RaftPayload(99, "127.0.0.1:9091");
            IKVMessage msg = KVMessageFactory.createRaftRequestVoteReplyMessage(payload, true);
            byte[] buffer = KVService.getInstance().getKvMessageSerializer().serialize(msg);
            IKVMessage deserializedMsg = KVService.getInstance().getKvMessageSerializer().deserialize(buffer);

            test1 = deserializedMsg.getStatus() == IKVMessage.StatusType.RAFT_REQUEST_VOTE_REPLY;
            RaftPayload deserializedPayload = new RaftPayload(deserializedMsg.getKey());
            test2 = deserializedPayload.getTermNumber() == 99;
            test3 = deserializedPayload.getAddress().equals("127.0.0.1:9091");
            test4 = deserializedMsg.getValue().equals("true");
        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2
                && test3
                && test4;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);
            System.out.println("test3:" + test3);
            System.out.println("test4:" + test4);

        }
        assertTrue(success);
    }

    @Test
    public void testRaftAppendEntriesSerialization() {

        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false, test4 = false;
        try {
            RaftPayload payload = new RaftPayload(99, "127.0.0.1:9091");
            IKVMessage msg = KVMessageFactory.createRaftAppendEntriesMessage(payload, "127.0.0.1:9095");
            byte[] buffer = KVService.getInstance().getKvMessageSerializer().serialize(msg);
            IKVMessage deserializedMsg = KVService.getInstance().getKvMessageSerializer().deserialize(buffer);

            test1 = deserializedMsg.getStatus() == IKVMessage.StatusType.RAFT_APPEND_ENTRIES;
            RaftPayload deserializedPayload = new RaftPayload(deserializedMsg.getKey());
            test2 = deserializedPayload.getTermNumber() == 99;
            test3 = deserializedPayload.getAddress().equals("127.0.0.1:9091");
            test4 = deserializedMsg.getValue().equals("127.0.0.1:9095");
        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2
                && test3
                && test4;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);
            System.out.println("test3:" + test3);
            System.out.println("test4:" + test4);

        }
        assertTrue(success);
    }

    @Test
    public void testRaftAppendEntriesReplySerialization() {

        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false, test4 = false;
        try {
            RaftPayload payload = new RaftPayload(99, "127.0.0.1:9091");
            IKVMessage msg = KVMessageFactory.createRaftAppendEntriesReplyMessage(payload, true);
            byte[] buffer = KVService.getInstance().getKvMessageSerializer().serialize(msg);
            IKVMessage deserializedMsg = KVService.getInstance().getKvMessageSerializer().deserialize(buffer);

            test1 = deserializedMsg.getStatus() == IKVMessage.StatusType.RAFT_APPEND_ENTRIES_REPLY;
            RaftPayload deserializedPayload = new RaftPayload(deserializedMsg.getKey());
            test2 = deserializedPayload.getTermNumber() == 99;
            test3 = deserializedPayload.getAddress().equals("127.0.0.1:9091");
            test4 = deserializedMsg.getValue().equals("true");
        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2
                && test3
                && test4;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);
            System.out.println("test3:" + test3);
            System.out.println("test4:" + test4);

        }
        assertTrue(success);
    }

    @Test
    public void testEcsAddressSerialization() {

        Exception ex = null;
        boolean test1 = false;
        try {
            IKVMessage msg = KVMessageFactory.createEcsAddressMessage();
            byte[] buffer = KVService.getInstance().getKvMessageSerializer().serialize(msg);
            IKVMessage deserializedMsg = KVService.getInstance().getKvMessageSerializer().deserialize(buffer);
            test1 = deserializedMsg.getStatus() == IKVMessage.StatusType.ECS_ADDRESS;
        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);

        }
        assertTrue(success);
    }

    @Test
    public void testEcsAddressSuccessSerialization() {

        Exception ex = null;
        boolean test1 = false, test2 = false;
        try {
            IKVMessage msg = KVMessageFactory.createEcsAddressSuccessMessage("127.0.0.1:9095");
            byte[] buffer = KVService.getInstance().getKvMessageSerializer().serialize(msg);
            IKVMessage deserializedMsg = KVService.getInstance().getKvMessageSerializer().deserialize(buffer);
            test1 = deserializedMsg.getStatus() == IKVMessage.StatusType.ECS_ADDRESS_SUCCESS;
            test2 = deserializedMsg.getKey().equals("127.0.0.1:9095");
        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);

        }
        assertTrue(success);
    }


    @Test
    public void testRaftMetadata() {

        Exception ex = null;
        boolean test1 = false,
                test2 = false,
                test3 = false,
                test4 = false,
                test5 = false,
                test6 = false,
                test7 = false,
                test8 = false,
                test9 = false;
        try {

            HashRing metadata = new HashRing();
            metadata.addServer("127.0.0.1:9091");
            metadata.addServer("127.0.0.1:9092");
            metadata.addServer("127.0.0.1:9093");
            metadata.addServer("127.0.0.1:9094");
            metadata.addServer("127.0.0.1:9095");

            RaftMetadata raft = new RaftMetadata(metadata, ERaftRole.Follower);
            test1 = raft.getRole() == ERaftRole.Follower;
            raft.setRole(ERaftRole.Candidate);
            test2 = raft.getRole() == ERaftRole.Candidate;
            test3 = raft.getClusterSize() == 5;
            test4 = raft.getTerm() == 0;
            raft.setTerm(99);
            test5 = raft.getTerm() == 99;
            raft.incrementTerm();
            test6 = raft.getTerm() == 100;
            test7 = raft.tryVote(100) == true;
            test8 = raft.tryVote(100) == false;
            raft.setRole(ERaftRole.Leader);
            test9 = raft.getTimeoutDuration() == 1000;

        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2
                && test3
                && test4
                && test5
                && test6
                && test7
                && test8
                && test9;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);
            System.out.println("test3:" + test3);
            System.out.println("test4:" + test4);
            System.out.println("test5:" + test5);
            System.out.println("test6:" + test6);
            System.out.println("test7:" + test7);
            System.out.println("test8:" + test8);
            System.out.println("test9:" + test9);
        }
        assertTrue(success);
    }
    boolean raftTimeoutTest = false;
    @Test
    public void testRaftTimeout() {

        Exception ex = null;
        try {

            RaftTimeout raftTimeout = new RaftTimeout();
            raftTimeout.startTimeout(99, new Runnable() {
                @Override
                public void run() {
                    raftTimeoutTest = true;
                }
            });
            Thread.sleep(1000);

        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && raftTimeoutTest;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("raftTimeoutTest:" + raftTimeoutTest);
        }
        assertTrue(success);
    }

    @Test
    public void testRaftCountdown() {

        Exception ex = null;
        boolean test1 = false,
                test2 = false,
                test3 = false,
                test4 = false,
                test5 = false,
                test6 = false;
        try {
            RaftCoutdown raftCountdown1 = new RaftCoutdown(1);
            test1 = raftCountdown1.getQuorum() == 1;
            RaftCoutdown raftCountdown2 = new RaftCoutdown(2);
            test2 = raftCountdown2.getQuorum() == 2;
            RaftCoutdown raftCountdown3 = new RaftCoutdown(3);
            test3 = raftCountdown3.getQuorum() == 2;
            test4 = raftCountdown1.waitForVotes(1) == false;
            raftCountdown2.voteReceived();
            test5 = raftCountdown2.waitForVotes(1) == false;
            raftCountdown3.voteReceived();
            raftCountdown3.voteReceived();
            test6 = raftCountdown3.waitForVotes(1) == true;

        }
        catch (Exception e) {
            ex = e;
        }

        boolean success = ex == null
                && test1
                && test2
                && test3
                && test4
                && test5
                && test6;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("test1:" + test1);
            System.out.println("test2:" + test2);
            System.out.println("test3:" + test3);
            System.out.println("test4:" + test4);
            System.out.println("test5:" + test5);
            System.out.println("test6:" + test6);
        }
        assertTrue(success);
    }
}
