package edu.ucsd.cs.triton.window;

import edu.ucsd.cs.triton.window.RingBuffer;

public class TestRingBuffer<T> extends RingBuffer<T> {
	
	private int _capacity;
	
	public TestRingBuffer(int capacity) {
		super();
		this._capacity = capacity;
	}
	
	@Override
  protected boolean removeEldestEntry(T eldest) {
		return this.count() >= this._capacity;
	}
	
	public static void main(String[] args) {
		TestRingBuffer<Integer> testRingBuffer = new TestRingBuffer<Integer> (10);
		
		for (int i = 0; i < 20; i++) {
			testRingBuffer.add(i);
		}
		
		System.out.println(testRingBuffer);
	}
}
