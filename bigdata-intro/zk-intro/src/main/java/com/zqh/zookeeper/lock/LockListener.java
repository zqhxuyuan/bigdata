package com.zqh.zookeeper.lock;

/**
 * http://blog.csdn.net/u014783753/article/details/44828889
 */
public interface LockListener {
    /**
     * call back called when the lock
     * is acquired
     */
    public void lockAcquired();

    /**
     * call back called when the lock is
     * released.
     */
    public void lockReleased();
}