package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;


public class IntSumReducer implements ReduceFunction<Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3295863172829143768L;

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return v1+v2;
	}

}
