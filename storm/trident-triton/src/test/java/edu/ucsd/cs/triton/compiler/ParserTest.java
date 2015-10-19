package edu.ucsd.cs.triton.compiler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import parser.ASTStart;
import parser.ParseException;
import parser.TritonParser;

public class ParserTest {
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		String fileName = "src/test/jjtree/test.esp";

		try {
			TritonParser tritonParser;
			tritonParser = new TritonParser(new FileInputStream(new File(
			    fileName)));

			ASTStart root;

			root = tritonParser.Start();
			root.dump(">");
			
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
