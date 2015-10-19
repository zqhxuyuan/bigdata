package edu.ucsd.cs.triton.expression;

import java.util.ArrayList;
import java.util.List;

public class LogicExpression extends BooleanExpression {
	private LogicOperator _op;
	
	public static LogicExpression createAndExpression() {
		return new LogicExpression(LogicOperator.AND);
	}
	
	public static LogicExpression createOrExpression() {
		return new LogicExpression(LogicOperator.OR);
	}
	
	public static LogicExpression createNotExpression() {
		return new LogicExpression(LogicOperator.NOT);
	}
	
	public LogicExpression(LogicOperator op) {
		super();
		_op = op;
	}
	
	public static BooleanExpression fromAndList(List<BooleanExpression> expList) {
		
		int n = expList.size();

		if (n == 0) {
			System.err.println("warning! empty exp list");
		}
		if (n == 1) {
			return expList.get(0);
		}
		
		BooleanExpression root = LogicExpression.createAndExpression();
		root._left = expList.get(0);
		root._right = expList.get(1);
		BooleanExpression cur = root;
		for (int i = 2; i < n; i++) {
			root = LogicExpression.createAndExpression();
			root._left = cur;
			cur._right = expList.get(i);
			cur = root;
		}
		
		return root;
	}
	
	public LogicExpression(LogicOperator op, BooleanExpression left, BooleanExpression right) {
		_left = left;
		_right = right;
		_op = op;
	}

	public LogicOperator getOperator() {
	  // TODO Auto-generated method stub
	  return _op;
  }
	
	public List<BooleanExpression> toAndList() {
		List<BooleanExpression> list = new ArrayList<BooleanExpression> ();
		
		if (_left == null || _right == null) {
			System.err.println("error in logic expression!");
			System.exit(1);
		}
		
		if (_left instanceof LogicExpression && ((LogicExpression) _left)._op == LogicOperator.AND) {
			list = ((LogicExpression) _left).toAndList();
		} else {
			list.add((BooleanExpression) _left.clone());
		}
		
		if (_right instanceof LogicExpression && ((LogicExpression) _right)._op == LogicOperator.AND) {
			List<BooleanExpression> rightList = ((LogicExpression) _right).toAndList();
			list.addAll(rightList);
		} else {
			list.add((BooleanExpression) _right.clone());
		}
		
		return list;
	}
	
	public LogicExpression clone() {
		return new LogicExpression(_op, (BooleanExpression) _left.clone(), (BooleanExpression) _right.clone());
	}
	
	@Override
	public void dump(String prefix) {
		System.out.println(prefix + _op);
		_left.dump(prefix + " ");
		_right.dump(prefix + " ");
	}
}
