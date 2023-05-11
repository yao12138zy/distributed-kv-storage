package testing.helper;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVBotTestingPlan {
    private int getOperations;
    private int putOperations;
    StringBuilder log = new StringBuilder();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock writeLock = readWriteLock.writeLock();
    private Lock readLock = readWriteLock.readLock();
    long totalTime = 0;
    int reports = 0;
    int clients = 0;
    public KVBotTestingPlan(int getOperations, int putOperations) {
        this.getOperations = getOperations;
        this.putOperations = putOperations;
    }
    public void setPlan(int getOperations, int putOperations) {
        this.getOperations = getOperations;
        this.putOperations = putOperations;
    }
    public int getGetOperations() {
        return getOperations;
    }
    public int getPutOperations() {
        return putOperations;
    }
    public void reportLog(String text) {
        try {
            writeLock.lock();
            log.append(text + "\r\n");
        }
        finally {
            writeLock.unlock();
        }
    }
    public void waitAll(){
        try {
            while(true) {
                try {
                    readLock.lock();
                    if (reports == clients) {
                        break;
                    }
                }
                finally {
                    readLock.unlock();
                }
                Thread.sleep(1000);
            }
        }
        catch (InterruptedException e){

        }
    }
    public void report(long duration){
        try{
            writeLock.lock();
            totalTime += duration;
            reports += 1;
        }
        finally {
            writeLock.unlock();
        }
    }
    public long getAverageDuration(){
        try{
            readLock.lock();
            return totalTime / reports;
        }
        finally {
            readLock.unlock();
        }
    }
    public String getLog(){
        try {
            readLock.lock();
            return log.toString();
        }
        finally {
            readLock.unlock();
        }
    }
    public void reset(int clients){
        try {
            writeLock.lock();
            log.setLength(0);
            totalTime = 0;
            reports = 0;
            this.clients = clients;
        }
        finally {
            writeLock.unlock();
        }
    }
}
