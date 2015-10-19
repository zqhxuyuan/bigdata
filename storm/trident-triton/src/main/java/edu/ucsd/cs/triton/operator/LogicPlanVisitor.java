package edu.ucsd.cs.triton.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parser.ASTAdditiveExpression;
import parser.ASTAggregateAttribute;
import parser.ASTAggregateFunction;
import parser.ASTAttribute;
import parser.ASTAttributeDefList;
import parser.ASTAttributeDefinition;
import parser.ASTCmpOp;
import parser.ASTCondAnd;
import parser.ASTCondOr;
import parser.ASTCondPrime;
import parser.ASTConstant;
import parser.ASTCreateRelation;
import parser.ASTCreateStream;
import parser.ASTFile;
import parser.ASTFloatingLiteral;
import parser.ASTFromClause;
import parser.ASTGroupByClause;
import parser.ASTInsertClause;
import parser.ASTInteger;
import parser.ASTMultiplicativeExpression;
import parser.ASTName;
import parser.ASTOrderByAttribute;
import parser.ASTOrderByClause;
import parser.ASTOutputClause;
import parser.ASTQuery;
import parser.ASTReName;
import parser.ASTSelectAttribute;
import parser.ASTSelectClause;
import parser.ASTSource;
import parser.ASTSpout;
import parser.ASTStart;
import parser.ASTStreamDef;
import parser.ASTStreamFilter;
import parser.ASTStringLiteral;
import parser.ASTTimePeriod;
import parser.ASTTypeFloat;
import parser.ASTTypeInt;
import parser.ASTTypeString;
import parser.ASTTypeTimestamp;
import parser.ASTUnaryExpression;
import parser.ASTUnits;
import parser.ASTWhereClause;
import parser.ASTWinLength;
import parser.ASTWinTime;
import parser.ASTWinTimeBatch;
import parser.Node;
import parser.SimpleNode;
import parser.TritonParserVisitor;
import edu.ucsd.cs.triton.expression.ArithmeticExpression;
import edu.ucsd.cs.triton.expression.ArithmeticOperator;
import edu.ucsd.cs.triton.expression.Attribute;
import edu.ucsd.cs.triton.expression.AttributeExpression;
import edu.ucsd.cs.triton.expression.BaseExpression;
import edu.ucsd.cs.triton.expression.BooleanExpression;
import edu.ucsd.cs.triton.expression.ComparisonExpression;
import edu.ucsd.cs.triton.expression.ComparisonOperator;
import edu.ucsd.cs.triton.expression.FloatExpression;
import edu.ucsd.cs.triton.expression.IntegerExpression;
import edu.ucsd.cs.triton.expression.LogicExpression;
import edu.ucsd.cs.triton.expression.LogicOperator;
import edu.ucsd.cs.triton.expression.StringExpression;
import edu.ucsd.cs.triton.resources.AttributeType;
import edu.ucsd.cs.triton.resources.BaseDefinition;
import edu.ucsd.cs.triton.resources.DynamicSource;
import edu.ucsd.cs.triton.resources.QuerySource;
import edu.ucsd.cs.triton.resources.RelationDefinition;
import edu.ucsd.cs.triton.resources.ResourceManager;
import edu.ucsd.cs.triton.resources.StaticSource;
import edu.ucsd.cs.triton.resources.StreamDefinition;

public class LogicPlanVisitor implements TritonParserVisitor {

	//private static final Logger LOGGER = LoggerFactory.getLogger(LogicPlanVisitor.class);
	
	private ResourceManager _resourceManager;
	private Map<String, BaseLogicPlan> _logicPlanList;
	
	public LogicPlanVisitor(ResourceManager resourceManager) {
		_resourceManager = resourceManager;
		_logicPlanList = new HashMap<String, BaseLogicPlan> ();
	}
	
	public ArrayList<BaseLogicPlan> getLogicPlanList() {
		return new ArrayList<BaseLogicPlan> (_logicPlanList.values());
	}
	
	@Override
  public Object visit(SimpleNode node, Object data) {
	  // TODO Auto-generated method stub
		System.err.println("you should not come here!");
		System.exit(1);
		return null;
  }

	static int count = 0;
	@Override
  public Object visit(ASTStart node, Object data) {
		int numOfChildren = node.jjtGetNumChildren();
		for (int i = 0; i < numOfChildren; i++) {
			Node n = node.jjtGetChild(i);
			if (n instanceof ASTCreateStream || 
					n instanceof ASTCreateRelation ||
					n instanceof ASTQuery) {
				n.jjtAccept(this, data);
			} else {
				System.err.println("Not supported query.");
			}
		}
		
		return null;
	}

	@Override
  //TODO
	public Object visit(ASTCreateStream node, Object data) {
		checkNumOfChildren(node, 2, "[CreateStream]");
		
		String streamName = node.streamName;
		
		StreamDefinition streamDef = new StreamDefinition(streamName);
	
		// get source node
		ASTSource source = (ASTSource) node.jjtGetChild(node.jjtGetNumChildren() - 1);
		streamDef = (StreamDefinition) node.childrenAccept(this, streamDef);
		
		_resourceManager.addStream(streamDef);
		
		if (source.jjtGetChild(0) instanceof ASTQuery) {
			// TODO
		} else {
			LogicRegisterPlan logicPlan = new LogicRegisterPlan(streamName, streamDef);
		  _logicPlanList.put(streamName, logicPlan);
		}
		
		return null;
  }

	@Override
	// TODO
  public Object visit(ASTCreateRelation node, Object data) {
		
		checkNumOfChildren(node, 2, "[CreateRelation]");

		String relationName = node.relationName;

		RelationDefinition relationDef = new RelationDefinition(relationName);
		relationDef = (RelationDefinition) node.childrenAccept(this, relationDef);

		_resourceManager.addRelation(relationDef);
		
		LogicRegisterPlan logicPlan = new LogicRegisterPlan(relationName, relationDef);
	  _logicPlanList.put(relationName, logicPlan);
		
	  return null;
  }
	
	@Override
  public Object visit(ASTAttributeDefList node, Object data) {
	  // TODO Auto-generated method stub
	  return node.childrenAccept(this, data);
  }

	@Override
	/**
	 * input:  BaseDefinition
	 * output: 
	 */
  public Object visit(ASTAttributeDefinition node, Object data) {
	  // TODO Auto-generated method stub
		BaseDefinition definition = (BaseDefinition) data;

		// attribute name
		String name = (String) node.jjtGetChild(0).jjtAccept(this, data);
		// attribute type
		AttributeType type = (AttributeType) node.jjtGetChild(1).jjtAccept(this, data);
		definition.addAttribute(name, type);
		
	  return null;
  }
	
	@Override
	/**
	 * input:
	 * output: INT
	 */
  public Object visit(ASTTypeInt node, Object data) {
	  // TODO Auto-generated method stub
	  return AttributeType.INT;
  }

	@Override
  public Object visit(ASTTypeFloat node, Object data) {
	  // TODO Auto-generated method stub
	  return  AttributeType.FLOAT;
  }

	@Override
  public Object visit(ASTTypeString node, Object data) {
	  // TODO Auto-generated method stub
	  return AttributeType.STRING;
  }

	@Override
  public Object visit(ASTTypeTimestamp node, Object data) {
	  // TODO Auto-generated method stub
	  return AttributeType.TIMESTAMP;
  }

	@Override
  public Object visit(ASTSource node, Object data) {
	  // TODO Auto-generated method stub
		checkNumOfChildren(node, 1, "ASTSource");
		
		BaseDefinition definition = (BaseDefinition) data;

		Node child = node.jjtGetChild(0);
		
		if (child instanceof ASTSpout) {
			String spout = (String) child.jjtAccept(this, data);
			definition.setSource(new DynamicSource(spout));
		} else if (child instanceof ASTFile) {
			// static source from database or data set
			String filename = (String) child.jjtAccept(this, data);
			definition.setSource(new StaticSource(filename));			
		} else if (child instanceof ASTQuery) {
			// dynamic source from a stream query result
			ResourceManager resoureManager = ResourceManager.getInstance();
			String unnamedStream = resoureManager.allocateUnnamedStream();
			definition.setSource(new QuerySource(unnamedStream));
			child.jjtAccept(this, data);
		} else {
			//LOGGER.error("Error in source node!");
		}
		
	  return null;
  }

	@Override
  public Object visit(ASTQuery node, Object data) {
	  // TODO Auto-generated method stub
		String planName = null;
		LogicQueryPlan logicPlan = null;
		
		if (data instanceof BaseDefinition) {
			BaseDefinition definition = (BaseDefinition) data;
			planName = definition.getName();
			logicPlan = LogicQueryPlan.newNamedLogicPlan(planName);
		} else {
			planName = _resourceManager.allocateUnnamedQuery();
			logicPlan = LogicQueryPlan.newAnonymousLogicPlan(planName);
		}

		int numOfChildren = node.jjtGetNumChildren();
		Node selectNode = node.jjtGetChild(0);
		Node fromNode = node.jjtGetChild(1);
		
		// visit FROM clause before the SELECT clause
		fromNode.jjtAccept(this, logicPlan);
		selectNode.jjtAccept(this, logicPlan);
		
		// visit rest clause;
		for (int i = 2; i < numOfChildren; i++) {
			node.jjtGetChild(i).jjtAccept(this, logicPlan);
		}
		
		_logicPlanList.put(planName, logicPlan);
		
	  return null;
  }

	@Override
  public Object visit(ASTSelectClause node, Object data) {
	  // TODO Auto-generated method stub
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;
		Projection projectionOperator = logicPlan.getProjection();

		int numOfChildren = node.jjtGetNumChildren();
		
		// expand select *
		if (numOfChildren == 0) {
			Set<String> inputStreams = logicPlan.getInputStreams();
			for (String streamName : inputStreams) {
				String[] attributes = logicPlan.getStreamAttributes(streamName);
				String originalName = logicPlan.unifiyDefinitionId(streamName);
				for (String attribute : attributes) {
					projectionOperator.addField(new SimpleField(streamName, originalName + '.' + attribute));
				}
			}
		} else {
			node.childrenAccept(this, data);
		}
		
		return null;
	}

	@Override
  public Object visit(ASTSelectAttribute node, Object data) {

		int numOfChildren = node.jjtGetNumChildren();
		
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;

		Projection projectionOperator = logicPlan.getProjection();
		Aggregation aggregation = logicPlan.getAggregation();
		Node attributeNode = node.jjtGetChild(0);
		ProjectionField field = null;
		Aggregator aggregator = null;
		
		if (attributeNode instanceof ASTAdditiveExpression) {
			BaseExpression expression = (BaseExpression) attributeNode.jjtAccept(this, data);
			
			if (expression instanceof AttributeExpression) {
				expression = (AttributeExpression) expression;
				Attribute attribute = ((AttributeExpression) expression).getAttribute();
				field = new SimpleField(attribute);
			} else {
				field = new ExpressionField(expression, _resourceManager.allocateUnnamedField());
			}
		} else if (attributeNode instanceof ASTAggregateAttribute) {
			// create aggregator
			aggregator = (Aggregator) attributeNode.jjtAccept(this, data);
			String functionName = aggregator.getName();
			field = new AggregateField(functionName, aggregator.getOutputField());
		} else {
			System.err.println("error in select attribute");
		}
		
		// check rename
		Node renameNode = node.jjtGetChild(numOfChildren - 1);
		if (renameNode instanceof ASTReName) {
			String outputField = (String) renameNode.jjtAccept(this, data);
			field.setOutputField(outputField);

			if (aggregator != null) {
				aggregator.setOutputField(outputField);
			}
		}
		
		if (aggregator != null) {
			aggregation.addAggregator(aggregator);
		}
		
		projectionOperator.addField(field);
		
		return null;
  }
	
	@Override
	/**
	 * @return String[2] = {stream, attribute}
	 */
  public Object visit(ASTAttribute node, Object data) {
	  // TODO Auto-generated method stub
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;
		return logicPlan.unifiyAttribute(node.name);
  }

	@Override
  public Object visit(ASTAggregateAttribute node, Object data) {
		
		int numOfChildren = node.jjtGetNumChildren();
		
		if (numOfChildren == 0) {
			// COUNT (*)
			return new Aggregator("count", null);
		} else {
			String aggregateFunction = (String) node.jjtGetChild(0).jjtAccept(this, data);
			String[] attribute = (String[]) node.jjtGetChild(1).jjtAccept(this, data);
			String inputField = attribute[1];
			return new Aggregator(aggregateFunction, inputField);
		}
  }
	
	@Override
  public Object visit(ASTFromClause node, Object data) {
		
		node.childrenAccept(this, data);
		
		return null;
	}

	@Override
  public Object visit(ASTStreamDef node, Object data) {
		
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;
		
		int numOfChildren = node.jjtGetNumChildren();
		
		// get stream name
		String originalName = (String) node.jjtGetChild(0).jjtAccept(this, data);
		// check stream if definition exists
		if (!_resourceManager.containsDefinition(originalName)) {
			System.err.println("stream def [" + originalName + "] not found!");
			return null;
		}
	  
		// generate input stream plan
		InputStream inputStream;
		
		// step 1. check rename
		Node lastNode = node.jjtGetChild(numOfChildren - 1);
		int searchRange = numOfChildren;
		if (lastNode instanceof ASTReName) {
			String rename = ((ASTReName) lastNode).rename;
			// add to rename set
			logicPlan.addRenameDefinition(originalName, rename);
			
			// create input stream with rename
			inputStream = new InputStream(originalName, rename);
			
			searchRange--;
		} else {
			// create input stream
			inputStream = new InputStream(originalName);
		}
		
		// step 2. get pre-window filter and window spec
		BaseWindow windowOperator = null;
		Selection selectionOperator = null;
		for (int i = 1; i < searchRange; i++) {
			Node childNode = node.jjtGetChild(i);
			if (childNode instanceof ASTStreamFilter) {
				Set<String> scope = new HashSet<String> ();
				scope.add(originalName);
				logicPlan.setCurrentScope(scope);
				selectionOperator = (Selection) childNode.jjtAccept(this, logicPlan);
				logicPlan.resetScope();
			} else if (childNode instanceof ASTWinLength || 
					       childNode instanceof ASTWinTime || 
					       childNode instanceof ASTWinTimeBatch) {
				windowOperator = (BaseWindow) childNode.jjtAccept(this, logicPlan);
				windowOperator.setInputStream(originalName);
			} else {
				System.err.println("Err");
				return null;
			}
		}
		
		// step3. build input stream plan
		BasicOperator currentOperator = inputStream;
		if (selectionOperator != null) {
			selectionOperator.addChild(inputStream, 0);
			inputStream.setParent(selectionOperator);
			currentOperator = selectionOperator;
		}
		
		if (windowOperator != null) {
			windowOperator.addChild(currentOperator, 0);
			currentOperator.setParent(windowOperator);
			currentOperator = windowOperator;
		}
		
		// step 4. add input stream into from-list, update scope
		if (inputStream.hasRename()) {
			logicPlan.addInputStream(inputStream.getRename(), currentOperator);
		} else {
			logicPlan.addInputStream(inputStream.getName(), currentOperator);
		}
		
		// step 5. update dependency list
		if (_logicPlanList.containsKey(originalName)) {
			logicPlan.addDependency(_logicPlanList.get(originalName));
		} else {
			System.err.println("error!");
		}
		
		return null;
  }
	
	@Override
  public Object visit(ASTStreamFilter node, Object data) {

		BooleanExpression filter = (BooleanExpression) node.jjtGetChild(0).jjtAccept(this, data);
		
		return new Selection(filter);
  }
	
	@Override
  public Object visit(ASTWinLength node, Object data) {
	  // TODO Auto-generated method stub
		int length = ((ASTInteger) node.jjtGetChild(0)).value;
	  return new FixedLengthWindow(length);
  }

	@Override
  public Object visit(ASTWinTime node, Object data) {
	  // TODO Auto-generated method stub
		long timePeriod = (Integer) node.jjtGetChild(0).jjtAccept(this, data);
		
	  return new TimeWindow(timePeriod);
  }
	
	@Override
  public Object visit(ASTWinTimeBatch node, Object data) {
	  // TODO Auto-generated method stub
		long timePeriod = (Integer) node.jjtGetChild(0).jjtAccept(this, data);
		
	  return new TimeBatchWindow(timePeriod);
  }

	@Override
  public Object visit(ASTTimePeriod node, Object data) {
	  // TODO Auto-generated method stub
		int length = ((ASTInteger) node.jjtGetChild(0)).value;
		Unit unit = (Unit) node.jjtGetChild(1).jjtAccept(this, data);
	  return (length * unit.getValue());
  }
	
	@Override
  public Object visit(ASTUnits node, Object data) {
	  // TODO Auto-generated method stub
	  return Unit.fromString(node.unit);
  }

	@Override
  public Object visit(ASTWhereClause node, Object data) {
	  // TODO Auto-generated method stub
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;

		Selection selectionOperator = logicPlan.getSelection();
		
		BooleanExpression filter = (BooleanExpression) node.jjtGetChild(0).jjtAccept(this, data);
		
		selectionOperator.setFilter(filter);
		
		return null;
  }

	@Override
  public Object visit(ASTCondPrime node, Object data) {
		BaseExpression left = (BaseExpression) node.jjtGetChild(0).jjtAccept(this, data);
		ComparisonOperator op = (ComparisonOperator) node.jjtGetChild(1).jjtAccept(this, data);
		BaseExpression right = (BaseExpression) node.jjtGetChild(2).jjtAccept(this, data);
		
	  return new ComparisonExpression(op, left, right);
  }

	@Override
  public Object visit(ASTCondAnd node, Object data) {
	  // TODO Auto-generated method stub
		BooleanExpression left = (BooleanExpression) node.jjtGetChild(0).jjtAccept(this, data);
		BooleanExpression right = (BooleanExpression) node.jjtGetChild(1).jjtAccept(this, data);
		
	  return new LogicExpression(LogicOperator.AND, left, right);
  }

	@Override
  public Object visit(ASTCondOr node, Object data) {
	  // TODO Auto-generated method stub
		BooleanExpression left = (BooleanExpression) node.jjtGetChild(0).jjtAccept(this, data);
		BooleanExpression right = (BooleanExpression) node.jjtGetChild(1).jjtAccept(this, data);
		
	  return new LogicExpression(LogicOperator.OR, left, right);
  }

	@Override
  public Object visit(ASTName node, Object data) {
	  // TODO Auto-generated method stub
	  return node.name;
  }

	@Override
  public Object visit(ASTGroupByClause node, Object data) {
	  // TODO Auto-generated method stub
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;
		Aggregation aggregation = logicPlan.getAggregation();
		int numOfChildren = node.jjtGetNumChildren();
		for (int i = 0; i < numOfChildren; i++) {
			String[] res = (String[]) node.jjtGetChild(i).jjtAccept(this, data);
			aggregation.addGroupByAttribute(new Attribute(res[0], res[1]));
		}
		
	  return null;
  }

	@Override
  public Object visit(ASTOrderByClause node, Object data) {
	  // TODO Auto-generated method stub
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;
		OrderBy order = logicPlan.getOrder();
		int numOfChildren = node.jjtGetNumChildren();
		for (int i = 0; i < numOfChildren - 1; i++) {
			OrderByAttribute attribute = (OrderByAttribute) node.jjtGetChild(i).jjtAccept(this, data);
			order.addOrderByAttribute(attribute);
		}
		
		// check if the last node is a limit node
		if (numOfChildren >= 2) {
			Node last = node.jjtGetChild(numOfChildren - 2);
			if (last instanceof ASTOrderByAttribute) {
				OrderByAttribute attribute = (OrderByAttribute) last.jjtAccept(this, data);
				order.addOrderByAttribute(attribute);
			} else if (last instanceof ASTInteger) {
				order.setLimit((Integer) last.jjtAccept(this, data));
			}
		}
		return null;
  }
	
	@Override
  public Object visit(ASTOrderByAttribute node, Object data) {
		String[] res = (String[]) node.jjtGetChild(0).jjtAccept(this, data);
	  return new OrderByAttribute(res[0], res[1], node.desc);
  }

	@Override
  public Object visit(ASTOutputClause node, Object data) {
	  // TODO Auto-generated method stub
		LogicQueryPlan logicPlan = (LogicQueryPlan) data;
		
		Node child = node.jjtGetChild(0);
		if (child instanceof ASTFile) {
			String fileName = (String) child.jjtAccept(this, data);
			fileName = fileName.substring(1, fileName.length()-1);
			logicPlan.setOutputStream(new OutputStream(fileName, logicPlan.getProjection().getOutputFieldList()));
		} else {
			//LOGGER.error("unsupported output stream.");
		}
	  
		return null;
  }
	
	@Override
  public Object visit(ASTCmpOp node, Object data) {
	  // TODO Auto-generated method stub
		return ComparisonOperator.fromString(node.op);
  }

	@Override
  public Object visit(ASTAggregateFunction node, Object data) {
	  // TODO Auto-generated method stub
	  return node.name;
  }
	
	@Override
  public Object visit(ASTAdditiveExpression node, Object data) {
	  // TODO Auto-generated method stub
		BaseExpression expression = null;
		
		int numOfChildren = node.jjtGetNumChildren();
		if (numOfChildren == 2) {
			BaseExpression left = (BaseExpression) node.jjtGetChild(0).jjtAccept(this, data);
			BaseExpression right = (BaseExpression) node.jjtGetChild(1).jjtAccept(this, data);
			
			ArithmeticOperator op = ArithmeticOperator.fromString(node.op);
			expression = new ArithmeticExpression(op, left, right);
		} else {
			expression = (BaseExpression) node.jjtGetChild(0).jjtAccept(this, data);
		}
		
	  return expression;
  }

	@Override
  public Object visit(ASTMultiplicativeExpression node, Object data) {
		
		int numOfChildren = node.jjtGetNumChildren();
		
		switch (numOfChildren) {
		case 1:
			return node.jjtGetChild(0).jjtAccept(this, data);
		case 2:
			BaseExpression left = (BaseExpression) node.jjtGetChild(0).jjtAccept(this, data);
			BaseExpression right = (BaseExpression) node.jjtGetChild(1).jjtAccept(this, data);
			ArithmeticOperator op = ArithmeticOperator.fromString(node.op);
			
			return new ArithmeticExpression(op, left, right);
		default:
			System.err.println("error in expression");
			System.exit(1);
		}

	  return null;
  }

	@Override
  public Object visit(ASTUnaryExpression node, Object data) {
		
		Node childNode = node.jjtGetChild(0);
		if (childNode instanceof ASTAttribute) {
			String[] attribute = (String[]) childNode.jjtAccept(this, data);
			
			return new AttributeExpression(new Attribute(attribute[0], attribute[1]));
		} else if (childNode instanceof ASTAdditiveExpression ||
				       childNode instanceof ASTConstant) {
			return childNode.jjtAccept(this, data);
		} else {
			System.err.println("type error in expression!");
			System.exit(1);
		}
	
		return null;
	}
	
	@Override
  public Object visit(ASTConstant node, Object data) {
	  
		Node childNode = node.jjtGetChild(0);
	  if (childNode instanceof ASTInteger) {
			return new IntegerExpression((Integer) childNode.jjtAccept(this, data));
		} else if (childNode instanceof ASTFloatingLiteral) {
			return new FloatExpression((Float) childNode.jjtAccept(this, data));
		} else if (childNode instanceof ASTStringLiteral) {
			return new StringExpression((String) childNode.jjtAccept(this, data));
		} else {
			System.err.println("type error in constant expression!");
			System.exit(1);
		}
	  
	  return null;
  }
	
	@Override
  public Object visit(ASTReName node, Object data) {
	  return node.rename;
  }

	@Override
  public Object visit(ASTStringLiteral node, Object data) {
	  return node.value;
  }

	@Override
  public Object visit(ASTInteger node, Object data) {
	  return node.value;
  }
	
	@Override
  public Object visit(ASTFloatingLiteral node, Object data) {
	  return node.value;
  }
	
	@Override
  public Object visit(ASTInsertClause node, Object data) {
	  // TODO Auto-generated method stub
		System.err.println("insert not implemented!");
		return null;
  }
	
	private void checkNumOfChildren(SimpleNode node, int num, String errMsg) {
		int numOfChild = node.jjtGetNumChildren();
		if (numOfChild != num) {
			System.err.println("Error: " + errMsg + " should have " + num
					+ " children, but only have " + numOfChild);
		}
	}

	@Override
  public Object visit(ASTSpout node, Object data) {
	  // TODO Auto-generated method stub
	  return node.name;
  }

	@Override
  public Object visit(ASTFile node, Object data) {
	  // TODO Auto-generated method stub
	  return node.uri;
  }
}