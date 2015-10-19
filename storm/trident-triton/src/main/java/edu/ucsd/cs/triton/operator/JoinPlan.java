package edu.ucsd.cs.triton.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class JoinPlan {
	public class StreamPair {
		private final String _left;
		private final String _right;

		StreamPair(final String left, final String right) {
			if (left.compareTo(right) > 0) {
				_left = right;
				_right = left;
			} else {
				_left = left;
				_right = right;
			}
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}

			if (!(o instanceof StreamPair)) {
				return false;
			}

			StreamPair pair = (StreamPair) o;

			return (pair._left.equals(_left) && pair._right.equals(_right));
		}

		@Override
		public int hashCode() {
			return _left.hashCode() + _right.hashCode();
		}
		
		@Override
		public String toString() {
			return "{" + _left + ", " + _right + "}";
		}
	}

	private Map<StreamPair, List<KeyPair>> _keyPairMap;
	private Map<String, Set<String>> _joinGraph;

	public JoinPlan() {
		_keyPairMap = new HashMap<StreamPair, List<KeyPair>> ();
		_joinGraph = new HashMap<String, Set<String>>();
	}

	public void addKeyPair(final KeyPair keyPair) {
		String left = keyPair.getLeftField().getStream();
		String right = keyPair.getRightField().getStream();
		StreamPair streamPair = new StreamPair(left, right);
		if (_keyPairMap.containsKey(streamPair)) {
			_keyPairMap.get(streamPair).add(keyPair);
		} else {
			List<KeyPair> keyPairList = new ArrayList<KeyPair> ();
			keyPairList.add(keyPair);
			_keyPairMap.put(streamPair, keyPairList);
		}
		
		buildGraph(keyPair);
	}
	
	public List<KeyPair> getJoinFields(final String leftStream, final String rightStream) {

		return _keyPairMap.get(new StreamPair(leftStream, rightStream));
	}
	
	public List<List<String>> getPartition() {
		System.out.println(_joinGraph);
	  // TODO Auto-generated method stub
		List<List<String>> partition = new ArrayList<List<String>> ();
		
		Stack<String> stack = new Stack<String> ();
		Set<String> visited = new HashSet<String> ();
	  for (String s : _joinGraph.keySet()) {
	  	if (!visited.contains(s)) {
	  		String cur = s;
	  		stack.push(cur);
	  		List<String> joinList = new ArrayList<String> ();
	  		joinList.add(cur);
	  		while(!stack.empty()) {
	  			cur = stack.pop();
		  		visited.add(cur);
	  			for (String neighbour : _joinGraph.get(cur)) {
	  				if (!visited.contains(neighbour)) {
	  					joinList.add(neighbour);
	  					stack.push(neighbour);
	  				}
	  			}
	  		}
	  		partition.add(joinList);
	  	}
	  }
		return partition;
  }
	
	private void buildGraph(final KeyPair keyPair) {
		String lhs = keyPair.getLeftField().getStream();
		String rhs = keyPair.getRightField().getStream();
		if (_joinGraph.containsKey(lhs)) {
			_joinGraph.get(lhs).add(rhs);
		} else {
			Set<String> set = new HashSet<String>();
			set.add(rhs);
			_joinGraph.put(lhs, set);
		}

		if (_joinGraph.containsKey(rhs)) {
			_joinGraph.get(rhs).add(lhs);
		} else {
			Set<String> set = new HashSet<String>();
			set.add(lhs);
			_joinGraph.put(rhs, set);
		}
	}

	public void addStream(String name) {
	  // TODO Auto-generated method stub
	  _joinGraph.put(name, new HashSet<String> ());
  }
}
