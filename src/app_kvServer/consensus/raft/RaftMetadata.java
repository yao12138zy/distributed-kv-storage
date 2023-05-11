package app_kvServer.consensus.raft;

import shared.Hash.IHashRing;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RaftMetadata {
    ERaftRole role;
    int term = 0;
    int timeoutDuration = 3000;
    final ReentrantReadWriteLock termLock = new ReentrantReadWriteLock();
    IHashRing metadata;
    int votedTerm = -1;
    public RaftMetadata(IHashRing metadata, ERaftRole role) {
        this.metadata = metadata;
        this.role = role;
        // random from 1000 to 2000
        int randMs = (int)(Math.random() * 1000) + 1000;
        this.timeoutDuration = 4000 + randMs;
    }
    public int getClusterSize() {
        return metadata.getHashRingMap().size();
    }
    public int getTerm() {
        try {
            termLock.readLock().lock();
            return term;
        }
        finally {
            termLock.readLock().unlock();
        }
    }
    public void setVotedTerm(int votedTerm) {
        try {
            termLock.writeLock().lock();
            if (votedTerm > this.votedTerm)
                this.votedTerm = votedTerm;
        }
        finally {
            termLock.writeLock().unlock();
        }
    }
    public boolean tryVote(int termNumber) {
        try {
            termLock.readLock().lock();
            if (termNumber > this.votedTerm) {
                this.votedTerm = termNumber;
                return true;
            }
            else
                return false;
        }
        finally {
            termLock.readLock().unlock();
        }
    }
    public void setTerm(int term) {
        try {
            termLock.writeLock().lock();
            this.term = term;
        }
        finally {
            termLock.writeLock().unlock();
        }
    }
    public int incrementTerm() {
        try {
            termLock.writeLock().lock();
            this.term++;
            return this.term;
        }
        finally {
            termLock.writeLock().unlock();
        }
    }
    public int getTimeoutDuration() {
        try {
            termLock.readLock().lock();
            if (role == ERaftRole.Leader)
                return 1000;
            else
                return timeoutDuration;
        }
        finally {
            termLock.readLock().unlock();
        }
    }
    public void setTimeoutDuration(int timeoutDuration) {
        this.timeoutDuration = timeoutDuration;
    }
    public ERaftRole getRole() {
        return role;
    }
    public void setRole(ERaftRole role) {
        this.role = role;
    }
}
