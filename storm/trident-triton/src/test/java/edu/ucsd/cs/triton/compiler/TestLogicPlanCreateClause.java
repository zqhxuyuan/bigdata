package edu.ucsd.cs.triton.compiler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import parser.ASTStart;
import parser.ParseException;
import parser.TritonParser;
import edu.ucsd.cs.triton.operator.LogicPlanVisitor;
import edu.ucsd.cs.triton.resources.ResourceManager;

public class TestLogicPlanCreateClause {
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		String fileName = "src/test/jjtree/create.esp";

		try {
			TritonParser tritonParser;
			tritonParser = new TritonParser(new FileInputStream(new File(
			    fileName)));

			ASTStart root;

			root = tritonParser.Start();
			root.dump(">");
			
			ResourceManager resourceManager = ResourceManager.getInstance();
			
			LogicPlanVisitor logicPlanVisitor = new LogicPlanVisitor(resourceManager);
			
			root.childrenAccept(logicPlanVisitor, resourceManager);
			
			System.out.println(resourceManager.getStreamByName("s1"));
			
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
