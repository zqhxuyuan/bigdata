package examples.spouts;

public enum AlgorithmType {
	
		DECISIONTREEAUDIT {
			public String[] getDataSetInputRecords() {
				return auditDataSetInputRecords;
			}
		}, 
		
		GENERALREGRESSIONAUDIT {
			public String[] getDataSetInputRecords(){
				return auditDataSetInputRecords;
			}
		},
		
		NAIVEBAYESAUDIT {
			public String[] getDataSetInputRecords() {
				return auditDataSetInputRecords;
			}
		},
		
		NEURALNETWORKAUDIT {
			public String[] getDataSetInputRecords() {
				return auditDataSetInputRecords;
			}
		}, 
		
		RANDOMFORESTAUDIT {
			public String[] getDataSetInputRecords() {
				return auditDataSetInputRecords;
			}
		},
		SUPPORTVECTORMACHINEAUDIT {
			public String[] getDataSetInputRecords() {
				return auditDataSetInputRecords;
			}
		},
		KMEANSIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		},
		NEURALNETWORKIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		},
		DECISIONTREEIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		},
		GENERALREGRESSIONIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		}, 
		HIERARCHICALCLUSTERINGIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		},
		RANDOMFORESTIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		},
		REGRESSIONIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		}, 
		SUPPORTVECTORMACHINEIRIS {
			public String[] getDataSetInputRecords() {
				return irisDataSetInputRecords;
			}
		}, 
		NAIVEBAYESIRIS {
			public String[] getDataSetInputRecords() {
				return naiveBayesIrisDataSetInputRecords;
			}
		}, 
		GENERALREGRESSIONOZONE {
			public String[] getDataSetInputRecords() {
				return ozoneDataSetInputRecords;
			}
		}, 
		NEURALNETWORKOZONE {
			public String[] getDataSetInputRecords() {
				return ozoneDataSetInputRecords;
			}
		},
		RANDOMFORESTOZONE {
			public String[] getDataSetInputRecords(){
				return ozoneDataSetInputRecords;
			}
		},
		REGRESSIONOZONE {
			public String[] getDataSetInputRecords() {
				return ozoneDataSetInputRecords;
			}
		},
		SUPPORTVECTORMACHINEOZONE {
			public String[] getDataSetInputRecords() {
				return svmOzoneDataSetInputRecords;
			}
		};
		
		/*
		 * List of sample records
		 */
		private static String[] auditDataSetInputRecords = {
				"Consultant,HSgrad,Divorced,Repair,9608.48,Male,0,40",
				"Private,HSgrad,Married,Machinist,12475.84,Male,0,40",
				"SelfEmp,College,Married,Sales,32963.39,Male,0,40",
				"Private,College,Married,Executive,31534.97,Male,0,55",
				"PSLocal,Vocational,Divorced,Executive,182165.08,Female,0,40",
				"PSState,Bachelor,Divorced,Executive,70603.7,Male,0,40",
				"Private,HSgrad,Absent,Service,88125.97,Male,0,30",
				"Private,Yr11,Unmarried,Professional,260405.44,Male,0,35",
				"PSState,College,Absent,Executive,66139.36,Female,0,40",
				"Private,College,Unmarried,Service,81838,Female,0,72"
				};

		private static String[] irisDataSetInputRecords = { "4.6,3.1,1.5,0.2",
				"5.0,3.6,1.4,0.2", "5.4,3.9,1.7,0.4", "7.0,3.2,4.7,1.4",
				"6.4,3.2,4.5,1.5", "6.9,3.1,4.9,1.5", "5.5,2.3,4.0,1.3",
				"6.5,2.8,4.6,1.5", "6.3,3.3,6.0,2.5", "5.8,2.7,5.1,1.9" };

		private static String[] naiveBayesIrisDataSetInputRecords = {
				"4.6,3.1,1.5,0.2,pseudoValue", "5.0,3.6,1.4,0.2,pseudoValue",
				"5.4,3.9,1.7,0.4,pseudoValue", "7.0,3.2,4.7,1.4,pseudoValue",
				"6.4,3.2,4.5,1.5,pseudoValue", "6.9,3.1,4.9,1.5,pseudoValue",
				"5.5,2.3,4.0,1.3,pseudoValue", "6.5,2.8,4.6,1.5,pseudoValue",
				"6.3,3.3,6.0,2.5,pseudoValue", "5.8,2.7,5.1,1.9,pseudoValue" };

		private static String[] ozoneDataSetInputRecords = { "4,58,5000,111",
				"5,59,69,3044,116", "6,51,3641,87", "9,53,111,153", "6,59,597,214",
				"6,64,1791,182", "11,63,793,188", "10,63,531,244", "7,62,419,243",
				"12,63,816,190", "9,54,3651,95" };

		private static String[] svmOzoneDataSetInputRecords = { "58,5000,111",
				"59,69,3044,116", "51,3641,87", "53,111,153", "59,597,214",
				"64,1791,182", "63,793,188", "63,531,244", "62,419,243",
				"63,816,190", "54,3651,95" };

		public abstract String[] getDataSetInputRecords();
	};
