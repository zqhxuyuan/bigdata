package edu.ucsd.cs.triton.operator;

import java.util.ArrayList;
import java.util.List;

public class Projection extends BasicOperator {
	private List<ProjectionField> _projectionFieldList;
	private String[] _outputFieldList = null;
	
	public Projection() {
		_type = OperatorType.PROJECTION;
		_projectionFieldList = new ArrayList<ProjectionField> ();
	}
	
	public void addField(ProjectionField attribute) {
		_projectionFieldList.add(attribute);
	}
	
	public List<ProjectionField> getProjectionFieldList() {
		return _projectionFieldList;
	}
	
	public String[] getOutputFieldList() {
		
		if (_outputFieldList != null) return _outputFieldList;
		
		String[] outputFields = new String[_projectionFieldList.size()];
		for (int i = 0; i < _projectionFieldList.size(); i++) {
			outputFields[i] = _projectionFieldList.get(i).getOutputField();
		}
		
		return outputFields;
	}
	
	public String toString() {
		return super.toString() + _projectionFieldList.toString();
	}
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
