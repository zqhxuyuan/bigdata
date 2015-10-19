package edu.ucsd.cs.triton.codegen;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.text.WordUtils;

import edu.ucsd.cs.triton.codegen.language.ClassStatement;
import edu.ucsd.cs.triton.operator.Aggregation;
import edu.ucsd.cs.triton.operator.Aggregator;
import edu.ucsd.cs.triton.operator.BaseLogicPlan;
import edu.ucsd.cs.triton.operator.BasicOperator;
import edu.ucsd.cs.triton.operator.ExpressionField;
import edu.ucsd.cs.triton.operator.FixedLengthWindow;
import edu.ucsd.cs.triton.operator.InputStream;
import edu.ucsd.cs.triton.operator.Join;
import edu.ucsd.cs.triton.operator.KeyPair;
import edu.ucsd.cs.triton.operator.LogicQueryPlan;
import edu.ucsd.cs.triton.operator.OperatorVisitor;
import edu.ucsd.cs.triton.operator.OrderBy;
import edu.ucsd.cs.triton.operator.OrderByAttribute;
import edu.ucsd.cs.triton.operator.OutputStream;
import edu.ucsd.cs.triton.operator.Product;
import edu.ucsd.cs.triton.operator.Projection;
import edu.ucsd.cs.triton.operator.ProjectionField;
import edu.ucsd.cs.triton.operator.Register;
import edu.ucsd.cs.triton.operator.Selection;
import edu.ucsd.cs.triton.operator.Start;
import edu.ucsd.cs.triton.operator.TimeBatchWindow;
import edu.ucsd.cs.triton.operator.TimeWindow;
import edu.ucsd.cs.triton.resources.AbstractSource;
import edu.ucsd.cs.triton.resources.BaseDefinition;
import edu.ucsd.cs.triton.resources.DynamicSource;
import edu.ucsd.cs.triton.resources.QuerySource;
import edu.ucsd.cs.triton.resources.ResourceManager;
import edu.ucsd.cs.triton.resources.StaticSource;

public class QueryTranslator implements OperatorVisitor {
	
	//private static final Logger LOGGER = LoggerFactory.getLogger(QueryTranslator.class);
	
	private final BaseLogicPlan _logicPlan;
	private final String _planName;
	private final ResourceManager _resourceManager;
	private TridentProgram _program;
	
	private boolean _containsWindowOperator;
	
	public QueryTranslator(final BaseLogicPlan logicPlan, TridentProgram program) {
		_logicPlan = logicPlan;
		_planName = logicPlan.getPlanName();
		_resourceManager = ResourceManager.getInstance();
		_program = program;
		_containsWindowOperator = false;
	}
	
	@Override
  public Object visit(Start operator, Object data) {
		StringBuilder sb = (StringBuilder) data;

		operator.childrenAccept(this, sb);
		
		//TODO fix me! This is a hack on the style. trim the last '\n'
		if (sb.charAt(sb.length() - 1) == '\n') {
			sb.deleteCharAt(sb.length()-1);
		}
		
		return null;
  }

	@Override
  public Object visit(Register operator, Object data) {
	  // TODO Auto-generated method stub
		StringBuilder sb = (StringBuilder) data;
		
		BaseDefinition streamDef = operator.getDefinition();
		AbstractSource source = streamDef.getSource();
		String streamName = streamDef.getName();
		
		if (source instanceof StaticSource) {
			String fileName = Util.newStringLiteral(source.toString());
			String[] attribute = _logicPlan.getStreamAttributes(streamName);
			String[] fields = new String[attribute.length];
			for (int i = 0; i < fields.length; i++) {
				fields[i] = streamName + '.' + attribute[i];
			}
			String statement = TridentBuilder.newInstance("StaticFileSpout", 
																						        streamName, 
																						        fileName, 
																						        TridentBuilder.newFields(fields));
			sb.append(statement);
		} else if (source instanceof DynamicSource) {
			String spout = source.toString();
			sb.append(TridentBuilder.newInstance(spout, streamName));
		} else if (source instanceof QuerySource) {
			// TODO
			operator.childrenAccept(this, data);
		} else {
			//LOGGER.error("unknown source [" + source + "]" + "is found!");
			System.exit(1);
		}
		
	  return null;
  }

	@Override
  public Object visit(Projection operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);

		StringBuilder sb = (StringBuilder) data;
		
		List<ProjectionField> fieldList = operator.getProjectionFieldList();
		
		// check if projection fields contains expression
		List<ExpressionField> exprFieldList = new ArrayList<ExpressionField> ();
		for (ProjectionField field : fieldList) {
			if (field instanceof ExpressionField) {
				exprFieldList.add((ExpressionField) field);
			}
		}

		// generate each function before projection
		if (!exprFieldList.isEmpty()) {
			EachTranslator eachTranslator = new EachTranslator(_planName, exprFieldList);
			ClassStatement eachFunction = eachTranslator.translate();
			_program.addInnerClass(eachFunction);
			
			// add each
			String inputFields = TridentBuilder.newFields(eachTranslator.getInputFields());
			String outputFields = TridentBuilder.newFields(eachTranslator.getOutputFields());
			String funcName = TridentBuilder.newFunction(eachTranslator.getName());
			sb.append(TridentBuilder.each(inputFields, funcName, outputFields));
		}
		
		// generate project fields
		String[] projectionFields = new String[fieldList.size()];
		for (int i = 0; i < fieldList.size(); i++) {
			projectionFields[i] = fieldList.get(i).getOutputField();
		}
		
		String fields = TridentBuilder.newFields(projectionFields);
		
		sb.append(TridentBuilder.project(fields));
	  
		return null;
  }

	@Override
  public Object visit(Selection operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);

		StringBuilder sb = (StringBuilder) data;

		//TODO generate Filter
		//LOGGER.info(operator.getFilter().toString());
		FilterTranslator filterTranslator = new FilterTranslator(_planName, operator.getFilter());
		
		ClassStatement filterClass = filterTranslator.translate();
		
		_program.addInnerClass(filterClass);
		
		// add filter
		String fields = TridentBuilder.newFields(filterTranslator.getFilterInputFields());
		String filter = TridentBuilder.newFunction(filterTranslator.getFilterName());
		sb.append(TridentBuilder.each(fields, filter));
	  return null;
  }

	@Override
  public Object visit(InputStream operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);
		
		if (!(_logicPlan instanceof LogicQueryPlan)) {
			System.err.println("error in input stream translation");
			System.exit(1);
		}	
		
		StringBuilder sb = (StringBuilder) data;
		
		String inputStream = operator.getName();
		String queryId = _planName + ((operator.hasRename())? operator.getRename() : inputStream);

		LogicQueryPlan queryPlan = (LogicQueryPlan) _logicPlan;
		if (queryPlan.isNamedQuery()) {
			// single input stream, no join/product
			sb.append("Stream " + _planName + " = ");
		} if (queryPlan.getInputStreams().size() > 1) {
			// multiple stream, intermediate stream
			sb.append("Stream " + queryId + " = ");
		}
		
		BaseDefinition streamDef = _resourceManager.getDefinitionByName(operator.getName());
		AbstractSource source = streamDef.getSource();
		if (source instanceof QuerySource) {
			sb.append(inputStream).append('\n');
		} else { // static / dynamic source 
			sb.append("_topology\n")
		  .append(TridentBuilder.newStream(queryId, inputStream));
		}
		
	  return null;
  }

	@Override
  public Object visit(Aggregation operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);
		
		StringBuilder sb = (StringBuilder) data;
		
		if (_containsWindowOperator) {
			operator.addWindowId("window");
		}
		// group by
		if (operator.containsGroupBy()) {
			String fields = TridentBuilder.newFields(operator.getGroupByList());
			sb.append(TridentBuilder.groupby(fields));
		}
		
		// aggregation
		sb.append(TridentBuilder.chainedAgg());
		for (Aggregator agg : operator.getAggregatorList()) {
			String aggregator = TridentBuilder.newFunction(WordUtils.capitalize(agg.getName()));
			String output = TridentBuilder.newFields(agg.getOutputField());
			//TODO fix the condition check
			if (aggregator.equals("count")) {
				sb.append(TridentBuilder.aggregate(aggregator, output));
			} else {
				String input = TridentBuilder.newFields(agg.getInputField());
				sb.append(TridentBuilder.aggregate(input, aggregator, output));
			}
		}
		sb.append(TridentBuilder.chainEnd());
	  return null;
  }

	@Override
  public Object visit(FixedLengthWindow operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);
		
		_containsWindowOperator = true;
		
		StringBuilder sb = (StringBuilder) data;

    String windowFactory = TridentBuilder.newFunction("FixedLengthSlidingWindow.Factory", Integer.toString(operator.getLength()));
    
    String inputStream = operator.getInputStream();
		BaseDefinition definiton = _resourceManager.getDefinitionByName(inputStream);
		Set<String> attributes = definiton.getAttributes().keySet();
		List<String> inputFields = new ArrayList<String> ();
		for (String attribute : attributes) {
			inputFields.add(inputStream + '.' + attribute);
		}
		String inputFieldsArg = TridentBuilder.newFields(inputFields);
		
		inputFields.add(0, "windowId");
		String outputFieldsArg = TridentBuilder.newFields(inputFields);
		
		String windowUpdater = TridentBuilder.newFunction("SlidingWindowUpdater");
		
		sb.append(TridentBuilder.partitionPersist(windowFactory, inputFieldsArg, windowUpdater, outputFieldsArg))
			.append(TridentBuilder.newValuesStream());
		return null;
  }

	@Override
  public Object visit(TimeWindow operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);
		
		_containsWindowOperator = true;
		
		StringBuilder sb = (StringBuilder) data;

    String windowFactory = TridentBuilder.newFunction("TimeSlidingWindow.Factory", Long.toString(operator.getDuration()));
    
    String inputStream = operator.getInputStream();
		BaseDefinition definiton = _resourceManager.getDefinitionByName(inputStream);
		Set<String> attributes = definiton.getAttributes().keySet();
		List<String> inputFields = new ArrayList<String> ();
		for (String attribute : attributes) {
			inputFields.add(inputStream + '.' + attribute);
		}
		String inputFieldsArg = TridentBuilder.newFields(inputFields);
		
		inputFields.add(0, "windowId");
		String outputFieldsArg = TridentBuilder.newFields(inputFields);
		
		String windowUpdater = TridentBuilder.newFunction("SlidingWindowUpdater");
		sb.append(TridentBuilder.partitionPersist(windowFactory, inputFieldsArg, windowUpdater, outputFieldsArg))
			.append(TridentBuilder.newValuesStream());
		return null;
  }

	@Override
	//TODO
  public Object visit(TimeBatchWindow operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);
		
		_containsWindowOperator = true;

		StringBuilder sb = (StringBuilder) data;

    String windowFactory = TridentBuilder.newFunction("FixedLengthSlidingWindow.Factory", Long.toString(operator.getDuration()));
		String inputFields = TridentBuilder.newFields();
		String outputFields = TridentBuilder.newFields();
		String windowUpdater = TridentBuilder.newFunction("SlidingWindowUpdater");
		sb.append(TridentBuilder.partitionPersist(windowFactory, inputFields, windowUpdater, outputFields))
			.append(TridentBuilder.newValuesStream());
	  return null;
  }

	@Override
  public Object visit(OutputStream operator, Object data) {
	  // TODO Auto-generated method stub
		operator.childrenAccept(this, data);
		
		StringBuilder sb = (StringBuilder) data;

		String outputFilter;
		if (operator.isStdout()) {
			outputFilter = TridentBuilder.newFunction("PrintFilter");
		} else {
			String fileName = Util.newStringLiteral(operator.getFileName());
			outputFilter = TridentBuilder.newFunction("FileWriteFilter", fileName);
		}
		
		String fields = TridentBuilder.newFields(operator.getOutputFieldList());
		sb.append(TridentBuilder.each(fields, outputFilter));		
	  
		return null;
  }
	
	@Override
  /**
   * We did not handle the case that join two same streams.
   */
	public Object visit(Join operator, Object data) {
	  // TODO Auto-generated method stub
		StringBuilder sb = (StringBuilder) data;
		int n = operator.getNumChildren();
    for (int i = 0; i < n; i++) {
    	StringBuilder localSb = new StringBuilder();
      operator.getChild(i).accept(this, localSb);
      Util.fixStyle(localSb);
      _program.addStmtToBuildQuery(localSb.toString());
    }
    
		String queryId = _planName + operator.getOutputDefinition();

		LogicQueryPlan queryPlan = (LogicQueryPlan) _logicPlan;
		if (queryPlan.isNamedQuery()) {
			// single input stream, no join/product
			sb.append("Stream " + _planName + " = ");
		} if (queryPlan.getInputStreams().size() > 1) {
			// multiple stream, intermediate stream
			sb.append("Stream " + queryId + " = ");
		}
		
		// gather join information
		String leftStream = operator.getLeftInputStream();
		String rightStream = operator.getRightInputStream();
		
		// gather join key
		List<String> leftJoinFields = new ArrayList<String> ();
		List<String> rightJoinFields = new ArrayList<String> ();

		List<KeyPair> keyPairList = operator.getJoinField();
		for (KeyPair keyPair : keyPairList) {
			leftJoinFields.add(keyPair.getFieldByStream(leftStream).getName());
			rightJoinFields.add(keyPair.getFieldByStream(rightStream).getName());
		}

		// gather the output field
		// step 1. we need keep the attribute of right side since it is a left-deep join tree.
		List<String> outputFields = new ArrayList<String> (rightJoinFields);

		LogicQueryPlan logicPlan = (LogicQueryPlan) _logicPlan;

		// step 2. find remaining fields in left stream and add to outputFields
		String[] attributes = logicPlan.getStreamAttributes(leftStream);
		String originalName = logicPlan.unifiyDefinitionId(leftStream);
		for (String attribute : attributes) {
			String field = originalName + '.' + attribute;
			if (!leftJoinFields.contains(field)) {
				outputFields.add(field);
			}
		}
		
		// step 2. find remaining fields in left stream and add to outputFields
		attributes = logicPlan.getStreamAttributes(rightStream);
		originalName = logicPlan.unifiyDefinitionId(rightStream);
		for (String attribute : attributes) {
			String field = originalName + '.' + attribute;
			if (!rightJoinFields.contains(field)) {
				outputFields.add(field);
			}
		}
		
		// build Trident join statement 
		String join = TridentBuilder.join(_planName+leftStream, 
				                              TridentBuilder.newFields(leftJoinFields), 
				                              _planName+rightStream, 
					                            TridentBuilder.newFields(rightJoinFields), 
					                            TridentBuilder.newFields(outputFields));
		
		sb.append("_topology\n")
		  .append(join);
	  
	  return null;
  }

	@Override
  public Object visit(Product operator, Object data) {
	  // TODO Auto-generated method stub
		StringBuilder sb = (StringBuilder) data;
		int n = operator.getNumChildren();
    for (int i = 0; i < n; i++) {
    	StringBuilder localSb = new StringBuilder();
      operator.getChild(i).accept(this, localSb);
      Util.fixStyle(localSb);
      _program.addStmtToBuildQuery(localSb.toString());
    }		
	  
		return null;
  }

	@Override
  public Object visit(BasicOperator operator, Object data) {
	  // TODO Auto-generated method stub
		System.err.println("should not come here!");
		operator.childrenAccept(this, data);

	  return null;
  }

	@Override
  public Object visit(OrderBy operator, Object data) {
		
		operator.childrenAccept(this, data);
		
		StringBuilder sb = (StringBuilder) data;
	  
		List<OrderByAttribute> attributeList = operator.getOrderByAttributeList();
		OrderByAttribute sortedField = attributeList.get(0);
		int limit = operator.getLimit();
		boolean desc = sortedField.getDesc();
		String firstN = TridentBuilder.newFunction("FirstN", 
				                                       Integer.toString(limit), 
				                                       Util.newStringLiteral(sortedField.getName()),
				                                       Boolean.toString(desc));
		
		sb.append(TridentBuilder.assembly(firstN));
		
		return null;
  }
}
