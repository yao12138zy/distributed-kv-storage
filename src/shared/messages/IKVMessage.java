package shared.messages;

public interface IKVMessage {
	
	public enum StatusType {

		FAILED,
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		JOIN_ECS,
		LEAVE_ECS,
		KEYRANGE,
		KEYRANGE_READ,
		SERVER_NOT_RESPONSIBLE,
		SERVER_WRITE_LOCK,
		KEYRANGE_SUCCESS,
		KEYRANGE_READ_SUCCESS,
		REQUEST_WRITE_LOCK,
		REQUEST_RELEASE_WRITE_LOCK,
		REQUEST_RELEASE_SHUTDOWN_LOCK,
		REPORT_TRANSFER_COMPLETE,
		KEYS_TRANSFER,
		RAFT_REQUEST_VOTE,
		RAFT_REQUEST_VOTE_REPLY,
		RAFT_APPEND_ENTRIES,
		RAFT_APPEND_ENTRIES_REPLY,
		ECS_ADDRESS,
		ECS_ADDRESS_SUCCESS,

	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


