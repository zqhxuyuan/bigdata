package com.github.disgear.zookeeper;

public interface ClosableThread {
	public void close();

	public boolean isClosed();
}
