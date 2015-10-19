package edu.ucsd.cs.triton.codegen;

import java.util.List;

import edu.ucsd.cs.triton.codegen.language.BlockStatement;
import edu.ucsd.cs.triton.codegen.language.JavaProgram;
import edu.ucsd.cs.triton.codegen.language.Keyword;
import edu.ucsd.cs.triton.codegen.language.MemberFunction;
import edu.ucsd.cs.triton.operator.BaseLogicPlan;
import edu.ucsd.cs.triton.operator.Start;
import edu.ucsd.cs.triton.resources.ResourceManager;

public final class CodeGenerator {
	private final List<BaseLogicPlan> _planList;
	
	private final String _className;
	private TridentProgram _program;
	
	public CodeGenerator(List<BaseLogicPlan> planList, final String fileName) {
	  // TODO Auto-generated constructor stub
		_planList = planList;
		_program = new TridentProgram(fileName);
		_className = fileName;
	}
	
	public JavaProgram generate() {
		
	  generateTopology();
	  
	  generateDefaultMainEntry();
		
		return _program.toJava();
	}

	private void generateTopology() {
		System.out.println(_planList);
		
		List<BaseLogicPlan> orderedPlanList = Util.tsort(_planList);
		
		System.out.println(orderedPlanList);
		
		for (BaseLogicPlan logicPlan : orderedPlanList) {
			StringBuilder sb = new StringBuilder();
			Start plan = logicPlan.generatePlan();
			plan.dump("");
			QueryTranslator translator = new QueryTranslator(logicPlan, _program);
			translator.visit(plan, sb);
			_program.addStmtToBuildQuery(sb.toString());
		}
  }

	// TODO
	private void generateDefaultMainEntry() {
	  // TODO Auto-generated method stub
		String classStmt = _className + " " + _className.toLowerCase() + " = " + Keyword.NEW + " " + _className + "()";
	  BlockStatement mainEntry = new MemberFunction("public static void main(String[] args)")
	  	.SimpleStmt(classStmt)
	  	.SimpleStmt(_className.toLowerCase() + ".execute(args)");
	  
	  _program.setDefaultMain((MemberFunction) mainEntry);
  }
}
