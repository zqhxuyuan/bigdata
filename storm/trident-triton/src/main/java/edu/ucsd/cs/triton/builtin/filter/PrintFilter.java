package edu.ucsd.cs.triton.builtin.filter;

import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * A filter that filters nothing but prints the tuples it sees. Useful to test and debug things.
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class PrintFilter implements Filter  {
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}
	
	@Override
	public void cleanup() {
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		System.out.println("stdout: " + tuple);
		return true;
	}
}
