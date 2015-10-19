package edu.ucsd.cs.triton.resources;

import java.util.HashMap;
import java.util.Map;

public class ResourceManager {
	private Map<String, BaseDefinition> _definitions;
	
	private static final ResourceManager INSTANCE = new ResourceManager();

	private static final String UNNAMED_STREAM_PREFIX = "unnamed_stream_";
	
	private static final String UNNAMED_FIELD_PREFIX = "unnamed_field_";

	private static final String UNNAMED_QUERY_PREFIX = "query";
	
	private static int unnamedStreamCount = 0;
	
	private static int unnamedFieldCount = 0;
	
	private static int unnamedQueryCount = 0;

	private ResourceManager() {
		this._definitions = new HashMap<String, BaseDefinition> ();
	}
	
	public static ResourceManager getInstance() {
		return INSTANCE;
	}
	
	public boolean addStream(final StreamDefinition stream) {
		if (_definitions.containsKey(stream)) {
			System.err.println("Duplicated stream [" + stream + "] is found.");
			System.exit(1);
		}
		return _definitions.put(stream.getName(), stream) != null;
	}
	
	public boolean addRelation(final RelationDefinition stream) {
		if (_definitions.containsKey(stream)) {
			System.err.println("Duplicated stream [" + stream + "] is found.");
			System.exit(1);
		}
		return _definitions.put(stream.getName(), stream) != null;
	}

	public StreamDefinition getStreamByName(final String name) {
		BaseDefinition def = _definitions.get(name);
		
		return (def instanceof StreamDefinition)? (StreamDefinition) def : null;
	}
	
	public RelationDefinition getRelationByName(final String name) {
		BaseDefinition def = _definitions.get(name);
		
		return (def instanceof RelationDefinition)? (RelationDefinition) def : null;
	}
	
	public BaseDefinition getDefinitionByName(final String name) {
		if (!_definitions.containsKey(name)) {
			System.err.println("definition [" + name + "] is not found!");
			System.exit(1);
		}
		return _definitions.get(name);
	}
	
	public Map<String, BaseDefinition> getDefinitions() {
		return _definitions;
	}
	
	public boolean containsDefinition(final String name) {
		return _definitions.containsKey(name);
	}
	
	public String allocateUnnamedStream() {
	  // TODO Auto-generated method stub
	  return (UNNAMED_STREAM_PREFIX + (unnamedStreamCount++));
  }
	
	public String allocateUnnamedField() {
		return (UNNAMED_FIELD_PREFIX + (unnamedFieldCount++));
	}

	public String allocateUnnamedQuery() {
	  // TODO Auto-generated method stub
		return (UNNAMED_QUERY_PREFIX + (unnamedQueryCount++));
  }

	public String getAttributeType(String stream, String attribute) {
	  // TODO Auto-generated method stub
		AttributeType type = _definitions.get(stream).getAttributeType(attribute);
		return type.toString();
  }
}
