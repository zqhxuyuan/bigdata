package edu.ucsd.cs.triton.codegen;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.cs.triton.expression.ArithmeticExpression;
import edu.ucsd.cs.triton.expression.Attribute;
import edu.ucsd.cs.triton.expression.AttributeExpression;
import edu.ucsd.cs.triton.expression.BaseExpression;
import edu.ucsd.cs.triton.expression.FloatExpression;
import edu.ucsd.cs.triton.expression.IntegerExpression;
import edu.ucsd.cs.triton.expression.StringExpression;
import edu.ucsd.cs.triton.operator.BaseLogicPlan;
import edu.ucsd.cs.triton.resources.ResourceManager;

public final class Util {
	private Util() {
	}
	
	
	//TODO fix me! This is a hack on the style. trim the last '\n'
	public static void fixStyle(StringBuilder sb) {
		if (sb.charAt(sb.length() - 1) == '\n') {
			sb.deleteCharAt(sb.length()-1);
		}
	}
	
	/**
	 * 
	 * @param arg
	 * @return append double quotes to the arg. arg -> "arg"
	 */
	public static String newStringLiteral(String arg) {
		return "\"" + arg + "\"";
	}
	
	public static String translateAttribute(final Attribute attribute, final Attribute[] inputFields) {
		
		int n = inputFields.length;
		for (int i = 0; i < n; i++) {
			if (attribute.equals(inputFields[i])) {
				ResourceManager resourceManager = ResourceManager.getInstance();
				String type = resourceManager.getAttributeType(attribute.getOriginalStream(), attribute.getAttributeName());
				return ("tuple.get" + type + "(" + i + ")");
			}
		}
		
		return null;
	}
	
	public static void translateArithmeticExpression(
      BaseExpression expression, Attribute[] inputFields, StringBuilder sb) {
	  // TODO Auto-generated method stub
		if (expression instanceof IntegerExpression ||
				expression instanceof FloatExpression ||
				expression instanceof StringExpression) {
			sb.append(expression.toString());
		} else if (expression instanceof AttributeExpression) {
			Attribute attribute = ((AttributeExpression) expression).getAttribute();
			sb.append(translateAttribute(attribute, inputFields));
			
		} else if (expression instanceof ArithmeticExpression) {
			ArithmeticExpression arithmeticExpression = (ArithmeticExpression) expression;

			sb.append("(");
			translateArithmeticExpression(arithmeticExpression.getLeft(), inputFields, sb);
			
			sb.append(" " + arithmeticExpression.getOperator() + " ");
			
			translateArithmeticExpression(arithmeticExpression.getRight(), inputFields, sb);
			sb.append(")");
		} else {
			System.err.println("error in arithmetic expr translation");
		}
  }
	
	public static List<BaseLogicPlan> tsort(final List<BaseLogicPlan> list) {
		Set<String> visited = new HashSet<String> ();
		Deque<BaseLogicPlan> stack = new ArrayDeque<BaseLogicPlan>();
		for (BaseLogicPlan cur : list) {
			if (!visited.contains(cur.getPlanName())) {
				tsortHelp(list, visited, stack, cur);
			}
		}
		
		List<BaseLogicPlan> res = new ArrayList<BaseLogicPlan> ();
		while (!stack.isEmpty()) {
			res.add(stack.pop());
		}
		
		Collections.reverse(res);
		
		return res;
	}
	
	private static <T> void tsortHelp(final List<BaseLogicPlan> list, Set<String> visited, Deque<BaseLogicPlan> stack, BaseLogicPlan cur) {
		visited.add(cur.getPlanName());
		Set<BaseLogicPlan> neighbor = cur.getDependenceList();
		for (BaseLogicPlan n : neighbor) {
			if (!visited.contains(n.getPlanName())) {
				tsortHelp(list, visited, stack, n);
			}
		}
		stack.push(cur);
	}
}
