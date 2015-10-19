package org.shirdrn.storm.api;

/**
 * An interface implemented by any class that has a defined, stateful,
 * lifecycle. Implementations of {@link LifecycleAware} conform to a standard method of
 * starting, stopping.
 * 
 * @author Yanjun
 */
public interface LifecycleAware {

	/**
	 * Starts a service or component.
	 * @throws InterruptedException
	 */
	void start();

	/**
	 * Stops a service or component.
	 * @throws InterruptedException
	 */
	void stop();
}
