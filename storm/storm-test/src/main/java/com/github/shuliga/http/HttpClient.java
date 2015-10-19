package com.github.shuliga.http;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.github.shuliga.commons.Constants;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

/**
 * User: yshuliga
 * Date: 08.01.14 12:25
 */
public class HttpClient {

	public static final String BASE_URL = "http://localhost:8080/api";
	public static final String APPLICATION_JSON = "application/json";
	public static final String TEXT_PLAIN = "text/plain";

	private final String baseUrl;
	private final Client client;
	private WebResource.Builder builder;

	public HttpClient(String baseUrl){
		this.baseUrl = baseUrl;
		ClientConfig clientConfig = new DefaultClientConfig();
		clientConfig.getClasses().add(JacksonJsonProvider.class);
		client = Client.create(clientConfig);
	}

	public HttpClient prepare(String path) {
		 builder = client.resource(baseUrl)
				.path(Constants.CATEGORIES_EVENT + path)
				.type(APPLICATION_JSON)
				.accept(APPLICATION_JSON);
		return  this;
	}

	public HttpClient prepare(String path, String paramName, String paramValue) {
		builder = client.resource(baseUrl)
				.path(Constants.CATEGORIES_EVENT + path)
				.queryParam(paramName, paramValue)
				.accept(APPLICATION_JSON);
		return  this;
	}

	public HttpClient prepare(String path, String accepts, String paramName, String paramValue) {
		builder = client.resource(baseUrl)
				.path(Constants.CATEGORIES_EVENT + path)
				.queryParam(paramName, paramValue)
				.accept(accepts);
		return  this;
	}

	public <T> T post(Class<T> clazz, Object entity){
		return builder.post(clazz, entity);
	}

	public void post(Object entity){
		builder.post(entity);
	}

	public <T> T get(Class<T> clazz){
		return builder.get(clazz);
	}

	public void head(){
		builder.head();
	}

}
