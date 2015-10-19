package edu.ucsd.cs.triton.resources;

public abstract class AbstractSource implements ISource {
	protected String _uri;
	
	public AbstractSource(final String uri) {
		_uri = uri.substring(1, uri.length()-1);
	}
	
	public String getSourceUri() {
		return _uri;
	}
	
	@Override
	public String toString() {
		return _uri;
	}
}
