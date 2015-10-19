package edu.ucsd.cs.triton.builtin.filter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * A filter that filters nothing but prints the tuples it sees. Useful to test and debug things.
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class FileWriteFilter implements Filter  {
	
	private final String _fileName;
	private PrintWriter _writer;
	
	public FileWriteFilter(String fileName) {
		_fileName = fileName;
	}
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		//System.out.println("open file");
		try {
	    _writer = new PrintWriter(_fileName);
		} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
    }
	}
	@Override
	public void cleanup() {
		//System.out.println("closed!!!!");
			_writer.close();
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		//System.out.println(tuple);
		_writer.println(tuple.getValues());
    _writer.flush();
		return true;
	}
}
