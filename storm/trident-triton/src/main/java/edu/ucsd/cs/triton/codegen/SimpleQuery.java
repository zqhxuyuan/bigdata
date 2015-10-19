package edu.ucsd.cs.triton.codegen;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

public class SimpleQuery implements IQuery {
	
  protected String _jobName;
	protected TridentTopology _topology;
  protected Config _conf;
  
  public SimpleQuery() {
  	_topology = new TridentTopology();
    _conf = new Config();
  }
  
  @Override
  public void init() {
  	_jobName = "local_test";
  	// TODO conf init
  	_conf.setDebug(false);
  }

  @Override
	public void buildQuery() {
	}

	@Override
  public void execute(String[] args) {
	  // local mode
		init();
		
		// build query
		buildQuery();
		
		// execute
		if (args.length == 0) {
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology(_jobName, _conf, _topology.build());
	  } else {
	  	// cluster mode
	    try {
	      StormSubmitter.submitTopology(args[0], _conf, _topology.build());
      } catch (AlreadyAliveException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
      } catch (InvalidTopologyException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
      }
	  }
  }
}

