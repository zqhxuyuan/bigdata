package org.shirdrn.storm.api.utils;

import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.IndicatorCalculator;
import org.shirdrn.storm.commons.utils.ReflectionUtils;

import com.google.common.collect.Maps;

/**
 * Factory for producing {@link IndicatorCalculator} instances.
 * 
 * @author Yanjun
 */
public class IndicatorCalculatorFactory {
	
	private static final Log LOG = LogFactory.getLog(IndicatorCalculatorFactory.class);
	private static final Map<Integer, IndicatorCalculator<?, ?, ?>> CALCULATORS = Maps.newHashMap();
	private static final Map<Class<?>, IndicatorCalculator<?, ?, ?>> CALCULATOR_CLASSES = Maps.newHashMap();
	
	private IndicatorCalculatorFactory() {
		super();
	}
	
	/**
	 * Register a {@link IndicatorCalculator} instance, which should be populated to
	 * a register table and for subsequence use. 
	 * @param calculatorClazz
	 */
	public static synchronized void registerCalculator(Class<?> calculatorClazz) {
		IndicatorCalculator<?, ?, ?> instance = CALCULATOR_CLASSES.get(calculatorClazz);
		if(instance == null) {
			instance = (IndicatorCalculator<?, ?, ?>) ReflectionUtils.newInstance(calculatorClazz);
			if(instance == null) {
				throw new RuntimeException("Fail to reflect class: " + calculatorClazz.getName());
			}
			int indicator = instance.getIndicator();
			CALCULATOR_CLASSES.put(calculatorClazz, instance);
			CALCULATORS.put(indicator, instance);
			LOG.info("Factory registered: " + String.format("%02d", indicator) + " <-> " + calculatorClazz.getName());
		}
	}

	/**
	 * Create a new {@link IndicatorCalculator} instance, here actually retrieve a 
	 * existed {@link IndicatorCalculator} instance from a register table and return to the caller. 
	 * @param indicator
	 * @return
	 * @throws NoSuchElementException
	 */
	public static IndicatorCalculator<?, ?, ?> newIndicatorCalculator(int indicator) throws NoSuchElementException {
		IndicatorCalculator<?, ?, ?> calculator = CALCULATORS.get(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for: indicator = " + indicator);
		}
		return calculator;
	}
	
}
