package edu.ucsd.cs.triton.expression;

public class Attribute {
	private final String _stream;
	private final String _name;
	
	/**
	 * 
	 * @param stream the stream that the attribute belongs to. If rename exists,
	 * it will be the renamed stream.
	 * @param name the attribute name in the format of
	 * "originalStreamName.attributeName"
	 */
	public Attribute(final String stream, final String name) {
		_stream = stream;
		_name = name;
	}

	/**
	 * get the stream name that the attribute belongs to.
	 * If the stream is renamed in the from clause, it will return the renamed
	 * stream name.
	 * @return
	 */
	public String getStream() {
	  // TODO Auto-generated method stub
	  return _stream;
  }
	
	public String getName() {
		return _name;
	}
	
	public String getDefaultOutputField() {
		return toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		
		if (o == null || !(o instanceof Attribute)) {
			return false;
		}
		
		Attribute attribute = (Attribute) o;
		
		return attribute._name.equals(_name) && attribute._stream.equals(_stream); 
	}
	
	@Override
	public String toString() {
		return _name;
	}

	public String getAttributeName() {
	  // TODO Auto-generated method stub
		String[] res = _name.split("\\.");
		return (res.length == 2)? _name.split("\\.")[1] : _name;
  }

	public String getOriginalStream() {
	  // TODO Auto-generated method stub
		String[] res = _name.split("\\.");
		return (res.length == 2)? _name.split("\\.")[0] : _name;

  }
}
