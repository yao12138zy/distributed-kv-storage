package shared;

import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import javax.xml.crypto.dsig.spec.XSLTTransformParameterSpec;

public final class KVMessageFactory {
    public static IKVMessage createMessage(IKVMessage.StatusType statusType, String key, String value) {
        KVMessage message = new KVMessage(statusType);
        message.setKey(key);
        message.setValue(value);
        return message;
    }
    public static IKVMessage createGetMessage(String key){
        KVMessage msg = new KVMessage(IKVMessage.StatusType.GET);
        msg.setKey(key);
        return msg;
    }
    public static IKVMessage createPutMessage(String key, String value){
        KVMessage msg = new KVMessage(IKVMessage.StatusType.PUT);
        msg.setKey(key);
        msg.setValue(value);
        return msg;
    }
    public static IKVMessage createGetSuccessMessage(String key, String value){
        KVMessage message = new KVMessage(IKVMessage.StatusType.GET_SUCCESS);
        message.setKey(key);
        message.setValue(value);
        return message;
    }
    public static IKVMessage createGetErrorMessage(String key){
        KVMessage message = new KVMessage(IKVMessage.StatusType.GET_ERROR);
        message.setKey(key);
        return message;
    }

    public static IKVMessage createPutSuccessMessage(String key, String value){
        KVMessage message = new KVMessage(IKVMessage.StatusType.PUT_SUCCESS);
        message.setKey(key);
        message.setValue(value);
        return message;
    }
    public static IKVMessage createPutUpdateMessage(String key, String value){
        KVMessage message = new KVMessage(IKVMessage.StatusType.PUT_UPDATE);
        message.setKey(key);
        message.setValue(value);
        return message;
    }
    public static IKVMessage createDeleteSuccessMessage(String key){
        KVMessage message = new KVMessage(IKVMessage.StatusType.DELETE_SUCCESS);
        message.setKey(key);
        return message;
    }

    public static IKVMessage createDeleteErrorMessage(String key){
        KVMessage message = new KVMessage(IKVMessage.StatusType.DELETE_ERROR);
        message.setKey(key);
        return message;
    }

    public static IKVMessage createPutErrorMessage(String key){
        KVMessage message = new KVMessage(IKVMessage.StatusType.PUT_ERROR);
        message.setKey(key);
        return message;
    }
    public static IKVMessage createFailedMessage(String description) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.FAILED);
        message.setValue(description);
        return message;
    }

    public static IKVMessage createServerNotResponsibleMessage(String key) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        //message.setKey(key);
        return message;
    }

    public static IKVMessage createServerWriteLockMessage() {
        KVMessage message = new KVMessage(IKVMessage.StatusType.SERVER_WRITE_LOCK);
        return message;
    }

    public static IKVMessage createKeyRangeMessage() {
        KVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE);
        return message;
    }
    public static IKVMessage createKeyRangeSuccessMessage(String mappingInfo) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE_SUCCESS);
        message.setKey(mappingInfo);
        message.setValue("");
        return message;
    }
    public static IKVMessage createJoinECSMessage(String hashIdentity) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.JOIN_ECS);
        message.setKey(hashIdentity);
        return message;
    }
    public static IKVMessage createLeaveECSMessage() {
        KVMessage message = new KVMessage(IKVMessage.StatusType.LEAVE_ECS);
        return message;
    }

    public static IKVMessage createRequestWriteLockMessage(int proposalNumber,  String metadata) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.REQUEST_WRITE_LOCK);
        message.setKey(proposalNumber + "");
        message.setValue(metadata);
        return message;
    }
    public static IKVMessage createRequestReleaseWriteLockMessage(int proposalNumber) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.REQUEST_RELEASE_WRITE_LOCK);
        message.setKey(proposalNumber + "");
        return message;
    }
    public static IKVMessage createRequestReleaseShutdownLockMessage() {
        KVMessage message = new KVMessage(IKVMessage.StatusType.REQUEST_RELEASE_SHUTDOWN_LOCK);
        return message;
    }
    public static IKVMessage createReportTransferCompleteMessage(int proposalNumber) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.REPORT_TRANSFER_COMPLETE);
        message.setKey(proposalNumber + "");
        return message;
    }

    // between servers, along with key-value pairs
    public static IKVMessage createKeyTransferMessage(String keyPairs) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.KEYS_TRANSFER);
        message.setKey("transfer");
        message.setValue(keyPairs);
        return message;
    }
    public static IKVMessage createKeyRangeReadMessage() {
        KVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE_READ);
        return message;
    }
    public static IKVMessage createKeyRangeReadSuccessMessage(String mappingInfo) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE_READ_SUCCESS);
        message.setKey(mappingInfo);
        message.setValue("");
        return message;
    }

    public static IKVMessage createRaftRequestVoteMessage(RaftPayload payload) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.RAFT_REQUEST_VOTE);
        message.setKey(payload.serialize());
        return message;
    }
    public static IKVMessage createRaftRequestVoteReplyMessage(RaftPayload payload, boolean voteGranted) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.RAFT_REQUEST_VOTE_REPLY);
        message.setKey(payload.serialize());
        message.setValue(String.valueOf(voteGranted));
        return message;
    }

    public static IKVMessage createRaftAppendEntriesMessage(RaftPayload payload, String ecsAddress) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.RAFT_APPEND_ENTRIES);
        message.setKey(payload.serialize());
        message.setValue(ecsAddress);
        return message;
    }
    public static IKVMessage createRaftAppendEntriesReplyMessage(RaftPayload payload, boolean success) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.RAFT_APPEND_ENTRIES_REPLY);
        message.setKey(payload.serialize());
        message.setValue(String.valueOf(success));
        return message;
    }

    public static IKVMessage createEcsAddressMessage() {
        KVMessage message = new KVMessage(IKVMessage.StatusType.ECS_ADDRESS);
        return message;
    }
    public static IKVMessage createEcsAddressSuccessMessage(String ecsConnectionString) {
        KVMessage message = new KVMessage(IKVMessage.StatusType.ECS_ADDRESS_SUCCESS);
        message.setKey(ecsConnectionString);
        return message;
    }
}
