package edu.ucsd.cs.triton;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parser.ASTStart;
import parser.ParseException;
import parser.TritonParser;
import edu.ucsd.cs.triton.codegen.CodeGenerator;
import edu.ucsd.cs.triton.codegen.language.JavaProgram;
import edu.ucsd.cs.triton.operator.BaseLogicPlan;
import edu.ucsd.cs.triton.operator.LogicPlanVisitor;
import edu.ucsd.cs.triton.resources.ResourceManager;

public class Compiler {
	private static final Logger LOGGER = LoggerFactory.getLogger(Compiler.class);

	public static void main(String[] args) {
		
		String inputFileName = "src/test/jjtree/trending_topic.tql";

		if (args.length > 0) {
			inputFileName = args[0];
		}

		try {
			LOGGER.info("Parsing the query script...");
			TritonParser tritonParser;
			tritonParser = new TritonParser(new FileInputStream(new File(
			    inputFileName)));

			LOGGER.info("Dump parsed AST...");
			ASTStart root = tritonParser.Start();
			root.dump(">");
			
			ResourceManager resourceManager = ResourceManager.getInstance();
			
			LogicPlanVisitor logicPlanVisitor = new LogicPlanVisitor(resourceManager);
			
			LOGGER.info("Generating logic plan...");
			root.jjtAccept(logicPlanVisitor, resourceManager);
			//System.out.println(resourceManager.getStreamByName("s1"));
			
			List<BaseLogicPlan> logicPlanList = logicPlanVisitor.getLogicPlanList();

			LOGGER.info("Generating trident code...");
			String className = FilenameUtils.removeExtension(inputFileName);
			className = FilenameUtils.getBaseName(className);
			className = WordUtils.capitalize(className);
			CodeGenerator codeGen = new CodeGenerator(logicPlanList, className);
			
			JavaProgram program = codeGen.generate();
			
			LOGGER.info("Translating trident code into java code...");
			String res = program.translate();
			
			System.out.println("result: " + res);
			
			LOGGER.info("Generating packge...");
			String path = "triton-codegen/src/main/java/";
			generatePackage(path, className.toLowerCase(), program);
			LOGGER.info("Compile success!");

		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
    }
	}
	
	private static void generatePackage(final String path, 
			                                final String pkg, 
			                                final JavaProgram program) throws IOException {
		String filePath = path + "/" + pkg + "/" + program.getProgramName() + ".java";
		FileUtils.writeStringToFile(new File(filePath), program.translate());
	}
}
