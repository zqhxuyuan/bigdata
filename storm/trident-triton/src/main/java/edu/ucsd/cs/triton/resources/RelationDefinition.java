package edu.ucsd.cs.triton.resources;

public class RelationDefinition extends BaseDefinition {
	
	public RelationDefinition(final String relationName) {
		super(relationName);
	}
	
	public RelationDefinition(final String relationName, final AbstractSource source) {
		super(relationName, source);
	}
}
