package edu.ucsd.cs.triton.compiler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import parser.ASTStart;
import parser.ParseException;
import parser.TritonParser;
import edu.ucsd.cs.triton.operator.BaseLogicPlan;
import edu.ucsd.cs.triton.operator.BasicOperator;
import edu.ucsd.cs.triton.operator.LogicPlanVisitor;
import edu.ucsd.cs.triton.operator.LogicQueryPlan;
import edu.ucsd.cs.triton.resources.ResourceManager;

public class TestLogicPlanBasicSelect {
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		String fileName = "src/test/jjtree/select.esp";

		try {
			TritonParser tritonParser;
			tritonParser = new TritonParser(new FileInputStream(new File(
			    fileName)));

			ASTStart root;

			root = tritonParser.Start();
			// root.dump(">");
			
			ResourceManager resourceManager = ResourceManager.getInstance();
			
			LogicPlanVisitor logicPlanVisitor = new LogicPlanVisitor(resourceManager);
			
			root.childrenAccept(logicPlanVisitor, resourceManager);
			
			System.out.println(resourceManager.getStreamByName("s1"));
			
			ArrayList<BaseLogicPlan> logicPlanList = logicPlanVisitor.getLogicPlanList();
			
			for (BaseLogicPlan logicPlan : logicPlanList) {
				logicPlan.dump();	
				BasicOperator plan = logicPlan.generatePlan();
				plan.dump("");
				System.out.println("------------------");
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
