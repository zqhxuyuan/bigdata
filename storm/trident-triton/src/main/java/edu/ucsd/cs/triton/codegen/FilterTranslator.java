package edu.ucsd.cs.triton.codegen;

import edu.ucsd.cs.triton.codegen.language.ClassStatement;
import edu.ucsd.cs.triton.codegen.language.Keyword;
import edu.ucsd.cs.triton.expression.ArithmeticExpression;
import edu.ucsd.cs.triton.expression.Attribute;
import edu.ucsd.cs.triton.expression.AttributeExpression;
import edu.ucsd.cs.triton.expression.BaseExpression;
import edu.ucsd.cs.triton.expression.BooleanExpression;
import edu.ucsd.cs.triton.expression.ComparisonExpression;
import edu.ucsd.cs.triton.expression.FloatExpression;
import edu.ucsd.cs.triton.expression.IntegerExpression;
import edu.ucsd.cs.triton.expression.LogicExpression;
import edu.ucsd.cs.triton.expression.StringExpression;


/**
 * filter looks like this
 * /*public static class S1PreWindowFilter implements Filter  {
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}
	@Override
	public void cleanup() {
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		return tuple.getInteger(0) == 3;
	}
}/*
 */
public class FilterTranslator {
	
	private static final String FILTER_PREFIX = "SelectionFilter";
	private static int filterCount = 0;
	
	private final BooleanExpression _filter;
	private final Attribute[] _inputFields;
	private final String _filterName;
	
	public FilterTranslator(final String planName, BooleanExpression filter) {
		_filter = filter;
		_inputFields = filter.getInputFields();
		_filterName = planName + FILTER_PREFIX + (filterCount++);
	}

	/**
	 * translate filter
	 * @return
	 */
	public ClassStatement translate() {
	  // TODO Auto-generated method stub
	  StringBuilder sb = new StringBuilder();
	  translateBooleanExpression(_filter, sb);
	  String isKeepStatement = sb.toString();
		
		return ClassStatement.createStaticClass(Keyword.PUBLIC, _filterName).Implements("Filter")
			.MemberFunction("public void prepare(Map conf, TridentOperationContext context)")
			.EndMemberFunction()
			.MemberFunction("public void cleanup()")
			.EndMemberFunction()
			.MemberFunction("public boolean isKeep(TridentTuple tuple)")
			.Return(isKeepStatement)
			.EndMemberFunction();
	}
	
	private void translateBooleanExpression(BaseExpression expression, StringBuilder sb) {
		if (expression instanceof IntegerExpression ||
				expression instanceof FloatExpression ||
				expression instanceof StringExpression) {
			sb.append(expression.toString());
		} else if (expression instanceof AttributeExpression) {
			Attribute attribute = ((AttributeExpression) expression).getAttribute();
		
			sb.append(Util.translateAttribute(attribute, _inputFields));
		
		} else if (expression instanceof ArithmeticExpression) {
			ArithmeticExpression arithmeticExpression = (ArithmeticExpression) expression;
			
			Util.translateArithmeticExpression(arithmeticExpression, _inputFields, sb);
			
		}	else if (expression instanceof ComparisonExpression) {
			ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
			
			translateBooleanExpression(comparisonExpression.getLeft(), sb);
			
			sb.append(" " + comparisonExpression.getOperator() + " ");
			
			translateBooleanExpression(comparisonExpression.getRight(), sb);
		} else if (expression instanceof LogicExpression) {
			LogicExpression logicExpression = (LogicExpression) expression;

			sb.append("(");
			translateBooleanExpression(logicExpression.getLeft(), sb);
			
			sb.append(" " + logicExpression.getOperator() + " ");
			
			translateBooleanExpression(logicExpression.getRight(), sb);
			sb.append(")");
		}
	}

	public Attribute[] getFilterInputFields() {
	  // TODO Auto-generated method stub
	  return _inputFields;
  }

	public String getFilterName() {
	  // TODO Auto-generated method stub
	  return _filterName;
  }
}

