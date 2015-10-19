package edu.ucsd.cs.triton.window;

import java.util.Iterator;
import java.util.LinkedList;

public class RingBuffer<T> implements Iterable<T> {
	
	private LinkedList<T> _ringBuffer;

	public RingBuffer() {
		_ringBuffer = new LinkedList<T> ();
	}
	
  public boolean add(T element) {
	  // TODO Auto-generated method stub
	  while (!_ringBuffer.isEmpty() && removeEldestEntry(_ringBuffer.getFirst())) {
	  	_ringBuffer.removeFirst();
	  }
	  
	  return _ringBuffer.add(element);
  }

  public boolean remove(T element) {
	  return _ringBuffer.remove(element);
  }

  public T get(int pos) {
	  // TODO Auto-generated method stub
	  return _ringBuffer.get(pos);
  }

  public int count() {
	  // TODO Auto-generated method stub
	  return _ringBuffer.size();
  }

	@Override
	public String toString() {
		return _ringBuffer.toString();
	}
  
  /**
	 * default policy: never remove any element.
	 */
  protected boolean removeEldestEntry(T eldest) {
	  // TODO Auto-generated method stub
	  return false;
  }

	@Override
  public Iterator<T> iterator() {
	  // TODO Auto-generated method stub
	  return this._ringBuffer.iterator();
  }
}
