/*
 * Copyright (c) 2011 University of Tartu
 */
package org.jpmml.example;

import java.io.*;

import org.dmg.pmml.*;

import com.beust.jcommander.Parameter;

public class CopyExample extends Example {

	@Parameter (
		names = {"--input"},
		description = "Input PMML file",
		required = true
	)
	private File input = null;

	@Parameter (
		names = {"--output"},
		description = "Output PMML file",
		required = true
	)
	private File output = null;


	static
	public void main(String... args) throws Exception {
		execute(CopyExample.class, args);
	}

	@Override
	public void execute() throws Exception {
		PMML pmml = IOUtil.unmarshal(this.input);

		IOUtil.marshal(pmml, this.output);
	}
}