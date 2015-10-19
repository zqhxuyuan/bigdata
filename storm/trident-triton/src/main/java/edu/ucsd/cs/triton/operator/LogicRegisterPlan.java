package edu.ucsd.cs.triton.operator;

import edu.ucsd.cs.triton.resources.BaseDefinition;

public class LogicRegisterPlan extends BaseLogicPlan {
	
	private final BaseDefinition _definition;
	
	public LogicRegisterPlan(final String planName, BaseDefinition def) {
		super(planName);
		_definition = def;
	}

	@Override
  public Start generatePlan() {
		Register registor = new Register(_definition);
		Start start = new Start();
		start.addChild(registor, 0);
		
	  return start;
  }
}
