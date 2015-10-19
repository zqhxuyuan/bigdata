package edu.ucsd.cs.triton.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.cs.triton.expression.Attribute;
import edu.ucsd.cs.triton.expression.AttributeExpression;
import edu.ucsd.cs.triton.expression.BooleanExpression;
import edu.ucsd.cs.triton.expression.ComparisonExpression;
import edu.ucsd.cs.triton.expression.LogicExpression;
import edu.ucsd.cs.triton.expression.LogicOperator;
import edu.ucsd.cs.triton.resources.BaseDefinition;
import edu.ucsd.cs.triton.resources.ResourceManager;

public class LogicQueryPlan extends BaseLogicPlan {

	//private static final Logger LOGGER = LoggerFactory.getLogger(LogicQueryPlan.class);
	
	private final boolean _isNamedQuery;
	private Map<String, String> _renameTable;
	private Set<String> _relations;

	private Map<String, BasicOperator> _inputStreams;
	private Projection _projection;
	private Selection _selection;
	private Aggregation _aggregation;
	private OrderBy _orderBy;
	private OutputStream _outputStream;
	
	private JoinPlan _joinPlan;

	private Set<String> _scope;

	private Set<String> _oldScope;
	
	private LogicQueryPlan(final String planName, final boolean isNamedQuery) {
		super(planName);
		_isNamedQuery = isNamedQuery;
		
		// input streams/relations
		_renameTable = new HashMap<String, String>();
		_inputStreams = new HashMap<String, BasicOperator>();
		_relations = new HashSet<String>();
		_scope = new HashSet<String> ();
		
		// operator
		_projection = new Projection();
		_selection = new Selection();
		_aggregation = new Aggregation();
		_orderBy = new OrderBy();
		
		// join rewrite
		_joinPlan = new JoinPlan();
		
		// output
		_outputStream = null;
	}

	public static LogicQueryPlan newNamedLogicPlan(final String planName) {
		return new LogicQueryPlan(planName, true);
	}
	
	public static LogicQueryPlan newAnonymousLogicPlan(final String planName) {
		return new LogicQueryPlan(planName, false);
	}
	
	public boolean addInputStream(final String name, final BasicOperator inputStream) {
		checkDuplication(name);
		
		// update scope;
		_scope.add(name);
		
		// update join plan stream list
		_joinPlan.addStream(name);
		
		return _inputStreams.put(name, inputStream) != null;
	}

	/*public boolean addRelation(final String name) {
		return _relations.add(name);
	}*/

	public String addRenameDefinition(final String name, final String rename) {
		checkDuplication(rename);
		
		return _renameTable.put(rename, name);
	}

	public String lookupRename(final String rename) {
		return _renameTable.get(rename);
	}
	
	public Set<String> getInputStreams() {
		return _inputStreams.keySet();
	}

	/**
	 * 
	 * @param streamName
	 * @return the attribute list as an array of the given streamName
	 */
	public String[] getStreamAttributes(final String streamName) {
		String originalName = unifiyDefinitionId(streamName);
		ResourceManager resourceManager = ResourceManager.getInstance();
		BaseDefinition definiton = resourceManager.getDefinitionByName(originalName);
		Set<String> attributes = definiton.getAttributes().keySet();
		
		return attributes.toArray(new String[attributes.size()]);
	}
	
	/**
	 * check if the stream is defined in the current scope
	 * @param name
	 * @return
	 */
	public boolean containsDefinition(final String name) {
		return _scope.contains(name);
	}

	/**
	 * 
	 * @param id
	 * @return if rename exists, return the original id
	 */
	public String unifiyDefinitionId(final String id) {
		// check rename, if exists replace it by the original name
		if (!containsDefinition(id)) {
			return null;
		}
		
		String originalId = lookupRename(id);
		if (originalId != null) {
			return originalId;
		} else if (containsDefinition(id)) {
			return id;
		} else {
			return null;
		}
	}

	/**
	 * 
	 * @param attribute
	 * @return String[2] = {stream, attribute}
	 */
	public String[] unifiyAttribute(final String attribute) {
		String[] res = attribute.split("\\.");
		ResourceManager resourceManager = ResourceManager.getInstance();

		// "attr" only
		if (res.length == 1) {
			String attributeName = attribute;
			boolean isFound = false;
			
			for (String streamId : _scope) {
				String originalStreamId = unifiyDefinitionId(streamId);
				BaseDefinition definition = resourceManager.getDefinitionByName(originalStreamId);
				if (definition.containsAttribute(attributeName)) {
					if (!isFound) {
						isFound = true;
						res = new String[2];
						res[0] = streamId;
						res[1] = originalStreamId + '.' + attributeName;
					} else {
						System.err.println("ambiguity in ["+ attribute + "]!");
						System.exit(1);
					}
				}
			}

			if (!isFound) {
				System.err.println("attribute [" + attribute + "] not found!");
				System.exit(1);
			}
		} else if (res.length == 2) {
			// "s.id"
			String name = unifiyDefinitionId(res[0]);
			
			if (name == null) {
				System.err.println("stream def [" + res[0] + "] not found!");
				System.exit(1);
			}
			
			BaseDefinition definition = resourceManager.getDefinitionByName(name);
			if (definition.containsAttribute(res[1])) {
				res[1] = name + '.' + res[1];
				return res;
			} else {
				System.err.println("attribute [" + res[1] + "] not found in [" + name
				    + "]");
				System.exit(1);
			}
		} else {
			System.err.println("error format in attribute!");
			System.exit(1);
		}

		return res;
	}

	public void setProjection(Projection projectionOperator) {
		// TODO Auto-generated method stub
		_projection = projectionOperator;
	}

	public Projection getProjection() {
		return _projection;
	}

	public Selection getSelection() {
		// TODO Auto-generated method stub
		return _selection;
	}

	public void setOutputStream(final OutputStream outputStream) {
		_outputStream = outputStream;
	}
	
	/**
	 * join detection, and selection push down.
	 */
	public BasicOperator rewriteJoin() {
		BooleanExpression filter = _selection.getFilter();

		List<BooleanExpression> andList;
		
		if (filter instanceof ComparisonExpression) {
			andList = new ArrayList<BooleanExpression> ();
			andList.add(filter);
		} else {
			andList = ((LogicExpression) filter).toAndList();
		}
		
		List<BooleanExpression> globalFilterList = new ArrayList<BooleanExpression>();
		Map<String, List<BooleanExpression>> localSelectionMap = new HashMap<String, List<BooleanExpression>>();
		
		for (BooleanExpression boolExp : andList) {

			// local filter, push down to local filter list
			if (boolExp.isFromSameDefiniton()) {
				// push down exp to local stream selection
				String definition = boolExp.getDefinition();
				if (localSelectionMap.containsKey(definition)) {
					List<BooleanExpression> localFilterList = localSelectionMap
					    .get(definition);
					localFilterList.add(boolExp);
				} else {
					List<BooleanExpression> localFilterList = new ArrayList<BooleanExpression>();
					localFilterList.add(boolExp);
					localSelectionMap.put(definition, localFilterList);
				}
			} else {
				if (boolExp instanceof ComparisonExpression) {
					ComparisonExpression cmpExp = (ComparisonExpression) boolExp;
					if (cmpExp.isJoinExpression()) {
						// add key pair to join plan
						Attribute left = ((AttributeExpression) cmpExp.getLeft())
						    .getAttribute();
						Attribute right = ((AttributeExpression) cmpExp.getRight())
						    .getAttribute();

						_joinPlan.addKeyPair(new KeyPair(left, right));
					} else {
						// insert into new Selection
						globalFilterList.add(cmpExp);
					}
				} else if (boolExp instanceof LogicExpression) {
					LogicExpression logicExp = (LogicExpression) boolExp;
					if (logicExp.getOperator() != LogicOperator.OR) {
						System.err.println("error in rewrite join, and appear!");
						System.exit(1);
					} else {
						// insert into new Selection
						globalFilterList.add(logicExp);
					}
				}
			}
		}
		// set new filter into selection operator
		if (globalFilterList.isEmpty()) {
			_selection.setFilter(null);
		} else {
			_selection.setFilter(LogicExpression.fromAndList(globalFilterList));
		}
		
		// set local filter for each definition
		for (Map.Entry<String, List<BooleanExpression>> entry : localSelectionMap.entrySet()) {
			BasicOperator op = _inputStreams.get(entry.getKey());
			Selection selection = new Selection(LogicExpression.fromAndList(entry.getValue()));
			selection.addChild(op, 0);
			_inputStreams.put(entry.getKey(), selection);
		}
		
		// create join
		// step 2: partition graph into join cluster
		List<List<String>> partition = _joinPlan.getPartition();
		System.out.println(partition);
		//LOGGER.info("Find partition: " + partition);
		
		// build join operator
		BasicOperator plan = constructProduct(partition);
		
		return plan;
	}
	
	public Start generatePlan() {
		
		//LOGGER.info("Generating plan...");
		Stack<BasicOperator> logicPlan = new Stack<BasicOperator> ();
		
		BasicOperator joinPlan = null;
		if (_inputStreams.size() == 1) {
			joinPlan = _inputStreams.values().iterator().next();
		} else {
			joinPlan = new Product();
			int i = 0;
			for (BasicOperator op : _inputStreams.values()) {
				joinPlan.addChild(op, i++);
			}
		}
		
		if (_selection.getFilter() != null && _inputStreams.size() > 1) {
			joinPlan = rewriteJoin();
		}
		
		logicPlan.push(joinPlan);
		//order:  output
		//           |
		//       projection
		//           |
		//       aggregation (group by)
		//           |
		//        selection
		//           |
		//         join
		
		// selection
		if (!_selection.isEmpty()) {
			logicPlan.push(_selection);
		}
		
		// aggregation
		if (!_aggregation.isEmpty()) {
			logicPlan.push(_aggregation);
		}	
		
		// order by
		if (!_orderBy.isEmpty()) {
			logicPlan.push(_orderBy);
		}
		
		// projection
		if (_projection != null) {
			logicPlan.push(_projection);
		}
		
		// unnamed query, set output
		if (_outputStream == null && !_isNamedQuery) {
			_outputStream = OutputStream.newStdoutStream(_projection.getOutputFieldList());
		}
		
		if (_outputStream != null) {
			logicPlan.push(_outputStream);
		}

		// append a start operator
		Start start = new Start();		
		BasicOperator op = logicPlan.pop();
		start.addChild(op, 0);
		while (!logicPlan.empty()) {
			op.addChild(logicPlan.pop(), 0);
			op = (BasicOperator) op.getChild(0);
		}
		
		return start;
	}

	public void dump() {
		System.out.println("rename table: " + _renameTable);
		System.out.println("input stream: " + _inputStreams);
		System.out.println("relations: " + _relations);
		System.out.println("projection: " + _projection);
		System.out.println("selection: ");
		_selection.dump();
		System.out.println("aggregation: " + _aggregation);
	}

	public Aggregation getAggregation() {
	  // TODO Auto-generated method stub
	  return _aggregation;
  }

	public OrderBy getOrder() {
	  return _orderBy;
  }
	
	public boolean isNamedQuery() {
		return _isNamedQuery;
	}

	private BasicOperator constructProduct(final List<List<String>> partition) {
		BasicOperator product = constructJoin(partition.get(0));
		for (int i = 1; i < partition.size(); i++) {
			Product newProduct = new Product();
			newProduct.addChild(product, 0);
			product.setParent(newProduct);
			BasicOperator right = constructJoin(partition.get(i));
			newProduct.addChild(right, 1);
			right.setParent(newProduct);
			product = newProduct;
		}
		
		return product;
	}
	
	/**
	 * construct left-deep join tree plan
	 * @param joinList
	 * @return
	 */
	private BasicOperator constructJoin(final List<String> joinList) {
		BasicOperator join = _inputStreams.get(joinList.get(0));
		for (int i = 1; i < joinList.size(); i++) {
			String leftStream;
			String rightStream = joinList.get(i);
			if (i == 1) {
				leftStream = joinList.get(0);
			} else {
				leftStream = ((Join) join).getOutputDefinition();
			}
			Join newJoin = new Join(leftStream, rightStream);
			List<KeyPair> joinFields = _joinPlan.getJoinFields(joinList.get(i-1), joinList.get(i));
			newJoin.setJoinFields(joinFields);
			newJoin.addChild(join, 0);
			BasicOperator right = _inputStreams.get(rightStream);
			newJoin.addChild(right, 1);
			join = newJoin;
		}
		
		return join;
	}

	/**
	 * change the current scope to @param scope
	 * @param scope
	 */
	public void setCurrentScope(Set<String> scope) {
	  // TODO Auto-generated method stub
	  _oldScope = _scope;
	  _scope = scope;
  }

	/**
	 * reset scope to the previous scope.
	 */
	public void resetScope() {
	  // TODO Auto-generated method stub
	  _scope = _oldScope;
  }
	
	/**
	 * check if definiton exists in the current scope (set of streams)
	 * @param id
	 */
	private void checkDuplication(String id) {
		if (_scope.contains(id)) {
			System.err.println("duplcated definition [" + id + "] is found!");
			System.exit(1);
		}
	}
}