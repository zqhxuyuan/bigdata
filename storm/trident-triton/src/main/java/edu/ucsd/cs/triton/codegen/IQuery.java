package edu.ucsd.cs.triton.codegen;

public interface IQuery {
	public void init();
	public void buildQuery();
	public void execute(String[] args);
}
