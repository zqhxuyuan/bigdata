package edu.ucsd.cs.triton.operator.language;

import edu.ucsd.cs.triton.codegen.language.JavaProgram;

public class TestJavaProgram {
	public static void main(String[] args) {
		JavaProgram program = new JavaProgram("name");
		
		program
			.Import()
				.add("hahah")
				.add("lolo")
			.EndImport()
			.Class()
				.InnerClass("Filter")
					.MemberFunction("public boolean filter(int a)")
						.If("a > 0")
							.SimpleStmt("fafasf")
							.SimpleStmt("hahah")
							.If("b > c")
								.SimpleStmt("bb")
							.EndIf()
							.SimpleStmt("lalal")
						.EndIf()
						.Return("true")
					.EndMemberFunction()
				.EndInnerClass()
				.SimpleStmt("int a")
				.SimpleStmt("int b")
				.MemberFunction("public static void main(String[] args)")
					.SimpleStmt("System.out.println(\"hello world\\n\")")
					.For("int a = 0; i < 3; i++")
						.SimpleStmt("hahahdd")
					.EndFor()
					.While("true")
						.SimpleStmt("fadsfsd")
						.SimpleStmt("baddsaf")
					.EndWhile()
					.Return("")
				.EndMemberFunction()
			.EndClass();
	
		//OutputStream outputStream = System.out;
		//Writer       writer       = new OutputStreamWriter(outputStream);
		System.out.println(program.translate());
	}
}
