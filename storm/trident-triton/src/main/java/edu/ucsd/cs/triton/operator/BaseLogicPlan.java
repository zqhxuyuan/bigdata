package edu.ucsd.cs.triton.operator;

import java.util.HashSet;
import java.util.Set;

import edu.ucsd.cs.triton.resources.BaseDefinition;
import edu.ucsd.cs.triton.resources.ResourceManager;

public abstract class BaseLogicPlan {
	protected final String _planName;
	protected Set<BaseLogicPlan> _dependenceList;
	
	BaseLogicPlan(final String planName) {
		_planName = planName;
		_dependenceList = new HashSet<BaseLogicPlan> ();
	}
	
	public Set<BaseLogicPlan> getDependenceList() {
		return _dependenceList;
	}
	
	public void addDependency(BaseLogicPlan plan) {
		_dependenceList.add(plan);
	}
	
	public String getPlanName() {
		return _planName;
	}
	
	/**
	 * 
	 * @param streamName
	 * @return the attribute list of the given streamName.
	 */
	public String[] getStreamAttributes(final String streamName) {
		ResourceManager resourceManager = ResourceManager.getInstance();
		BaseDefinition definiton = resourceManager.getDefinitionByName(streamName);
		Set<String> attributes = definiton.getAttributes().keySet();
		
		return attributes.toArray(new String[attributes.size()]);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		
		if (o == null || !(o instanceof BaseLogicPlan)) {
			return false;
		}
		
		BaseLogicPlan plan = (BaseLogicPlan) o;
		
		return _planName.equals(plan._planName);
	}
	
	@Override
	public String toString() {
		return _planName;
	}
	
	public abstract Start generatePlan();

	public void dump() {
	  // TODO Auto-generated method stub
	  
  }
}
