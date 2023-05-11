package app_kvServer;

import app_kvServer.consensus.raft.ERaftRole;
import app_kvServer.consensus.raft.RaftCoutdown;
import app_kvServer.consensus.raft.RaftMetadata;
import app_kvServer.consensus.raft.RaftTimeout;

import org.apache.log4j.Logger;

public class ConsensusHandler implements Runnable {
    private RaftMetadata raftMetadata;
    private final Logger logger = Logger.getRootLogger();

    RaftTimeout raftTimeout;
    KVServer kvServer;
    RaftCoutdown raftCoutdown;
    boolean stopping = false;
    public ConsensusHandler(RaftMetadata raftMetadata, KVServer kvServer) {
        this.raftMetadata = raftMetadata;
        this.kvServer = kvServer;

    }



    private void queueTimeout() {
        raftTimeout.startTimeout(raftMetadata.getTimeoutDuration(), new Runnable() {
            @Override
            public void run() {
                timeout();
            }
        });
    }

    private void timeout() {
        if (raftMetadata.getRole() == ERaftRole.Leader) {
            logger.info("Leader timeout, start appendEntries");
            kvServer.sendAppendEntries();
            if (!stopping) {
                queueTimeout();
            }
        }
        else {
            logger.info("Follower timeout, start leader election");
            startLeaderElection();
        }

    }

    public void receivedAppendEntries(int termNumber) {
        if (termNumber > raftMetadata.getTerm()) {
            raftMetadata.setTerm(termNumber);
            raftMetadata.setRole(ERaftRole.Follower);
            queueTimeout();
        }
        else if (termNumber == raftMetadata.getTerm()) {
            if (raftMetadata.getRole() != ERaftRole.Leader) {
                raftMetadata.setRole(ERaftRole.Follower);
                queueTimeout();
            }
            else {
                logger.info("appendEntriesReplyReceived: i'm the leader, ignore reply");
            }
        }
        else {
            logger.info("appendEntriesReplyReceived: termNumber < myTerm, ignore outdated reply");
        }
    }

    public boolean receivedRequestVote(int termNumber) {
        boolean voteGranted = false;
        if (termNumber > raftMetadata.getTerm()) {
            raftMetadata.setTerm(termNumber);
            raftMetadata.setRole(ERaftRole.Follower);
            queueTimeout();
            voteGranted = raftMetadata.tryVote(termNumber);
            System.out.println("receivedRequestVote: " + termNumber + " myTerm:" + raftMetadata.getTerm() + " Result:" + voteGranted);
            return voteGranted;
        }
        else if (termNumber == raftMetadata.getTerm()) {
            if (raftMetadata.getRole() == ERaftRole.Leader) {
                System.out.println("receivedRequestVote: " + termNumber + " myTerm:" + raftMetadata.getTerm() + " Result:" + voteGranted);
                return false;
            }
            else if (raftMetadata.getRole() == ERaftRole.Candidate) {
                System.out.println("receivedRequestVote: " + termNumber + " myTerm:" + raftMetadata.getTerm() + " Result:" + voteGranted);
                return false;
            }
            else {
                voteGranted = raftMetadata.tryVote(termNumber);
                System.out.println("receivedRequestVote: " + termNumber + " myTerm:" + raftMetadata.getTerm() + " Result:" + voteGranted);
                return voteGranted;
            }
        }
        else {
            return false;
        }
    }


    public void leaderVoteReceived(int termNumber) {
        if (raftCoutdown == null) {
            return;
        }
        if (termNumber > raftMetadata.getTerm()) {
            logger.error("leaderVoteReceived: termNumber > myTerm");
            raftMetadata.setTerm(termNumber);
            raftMetadata.setRole(ERaftRole.Follower);
            return;
        }
        else if (termNumber < raftMetadata.getTerm()) {
            logger.error("leaderVoteReceived: termNumber < myTerm, ignore outdated vote");
            return;
        }
        if (raftMetadata.getRole() == ERaftRole.Candidate) {
            raftCoutdown.voteReceived();
            logger.info("Leader election vote collected");

        }
        else {
            logger.error("leaderVoteReceived: not a candidate, ignore vote");
        }
    }

    public void startLeaderElection() {
        int myElectionTerm = raftMetadata.incrementTerm();
        raftMetadata.setRole(ERaftRole.Candidate);
        logger.info("Starting leader election, term:" + raftMetadata.getTerm());

        int clusterSize = raftMetadata.getClusterSize();
        if (clusterSize == 1) {
            promoteToLeader();
            queueTimeout();
            return;
        }
        raftCoutdown = new RaftCoutdown(clusterSize);
        this.kvServer.sendRequestVote(raftMetadata.getTerm());
        raftCoutdown.voteReceived(); // Vote for self
        raftMetadata.tryVote(myElectionTerm);
        int quorum = raftCoutdown.getQuorum();
        logger.info("Leader election collecting vote[" + raftMetadata.getTerm() + "]: 1/" + quorum);
        boolean success = raftCoutdown.waitForVotes(raftMetadata.getTimeoutDuration());
        if (success) {
            if (raftMetadata.getRole() == ERaftRole.Candidate) {
                promoteToLeader();
            }
            queueTimeout();
        }
        else {
            // Did not collect enough vote before timeout, if still candidate, start new election
            if (raftMetadata.getRole() == ERaftRole.Candidate) {
                logger.info("Did not collect enough vote before timeout");
                // random from 3000 to 6000
                int random = (int)(Math.random() * 3000 + 3000);
                raftTimeout.startTimeout(raftMetadata.getTimeoutDuration() + random, new Runnable() {
                    @Override
                    public void run() {
                        startLeaderElection();
                    }
                });
            }
            else {
                logger.info("Leader election stopped, no longer a candidate");
                queueTimeout();
            }
        }
    }

    private void promoteToLeader() {
        raftMetadata.setRole(ERaftRole.Leader);
        logger.info("Promoted to leader, term:" + raftMetadata.getTerm());
        kvServer.notifyElectedAsLeader();
    }

    public void run() {
        raftTimeout = new RaftTimeout();
        logger.info("Starting Consensus Handler, current role:" + raftMetadata.getRole());
        if (raftMetadata.getRole() == ERaftRole.Follower) {
            queueTimeout();
        }
        else if (raftMetadata.getRole() == ERaftRole.Leader) {
            startLeaderElection();
        }
        else {

        }
    }

    public void stop() {
        stopping = true;
    }
}
