package edu.ucsd.cs.triton.codegen;

public final class Import {
	public static final String[] DEFAULT = new String[] {
		"edu.ucsd.cs.triton.builtin.filter.*",
    "edu.ucsd.cs.triton.builtin.spout.*",
    "edu.ucsd.cs.triton.builtin.aggregator.*",
		"edu.ucsd.cs.triton.codegen.SimpleQuery",
    "edu.ucsd.cs.triton.window.*",
    "storm.trident.tuple.TridentTuple",
    "storm.trident.Stream",
    "storm.trident.operation.builtin.*",
    "backtype.storm.tuple.Fields",
		"backtype.storm.tuple.Values"
  };
	
	public static final String[] FILTER = new String[] {
		"storm.trident.operation.Filter",
		"storm.trident.operation.TridentOperationContext",
		"java.util.Map"
	};

	public static final String[] EACH = new String[] {
			"storm.trident.operation.BaseFunction",
			"storm.trident.operation.TridentCollector"
	};
	
	private Import() {}
}
