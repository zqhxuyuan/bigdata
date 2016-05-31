package com.zqh.paas.exception;

public class PropertyMissingException extends Exception {
	public PropertyMissingException(String propertyKey) {
		super("property config " + propertyKey + " is missing,please check your config!");
	}

	public PropertyMissingException() {
		super();
	}

	public PropertyMissingException(String propertyKey, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super("property config " + propertyKey + " is missing,please check your config!", cause, enableSuppression, writableStackTrace);
	}

	public PropertyMissingException(String propertyKey, Throwable cause) {
		super("property config " + propertyKey + " is missing,please check your config!", cause);
	}

	public PropertyMissingException(Throwable cause) {
		super(cause);
	}

}
