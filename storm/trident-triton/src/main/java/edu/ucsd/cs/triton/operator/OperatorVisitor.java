package edu.ucsd.cs.triton.operator;

public interface OperatorVisitor {
	public Object visit(BasicOperator operator, Object data);
	public Object visit(Start operator, Object data);
	public Object visit(Projection operator, Object data);
	public Object visit(Selection operator, Object data);
	public Object visit(InputStream operator, Object data);
	public Object visit(Join operator, Object data);
	public Object visit(Product operator, Object data);
	public Object visit(Aggregation operator, Object data);
	public Object visit(FixedLengthWindow operator, Object data);
	public Object visit(TimeWindow operator, Object data);
	public Object visit(TimeBatchWindow operator, Object data);
	public Object visit(OutputStream operator, Object data);
	public Object visit(Register operator, Object data);
	public Object visit(OrderBy operator, Object data);
}
