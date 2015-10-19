package com.github.shuliga.context;

import com.github.shuliga.security.Credentials;

import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * User: yshuliga
 * Date: 07.01.14 12:56
 */

@ApplicationScoped
public class AppContext {

	@Inject
	RegisteredCredentials registeredCredentials;

	private  Map<Credentials, String> registeredClients = new HashMap<Credentials, String>();

	public AppContext(){

	}

	@Lock(LockType.WRITE)
	public String registerClient(Credentials credentials){
		if (!registeredCredentials.contains(credentials)) {
			return null;
		}
		if (registeredClients.containsKey(credentials)) {
			return registeredClients.get(credentials);
		}

		String token = String.valueOf(credentials.hashCode());
		registeredClients.put(credentials, token);
		return token;
	}

	@Lock(LockType.WRITE)
	public void logoff(String token){
		Credentials credentials = getKeByValue(token, registeredClients);
		if (credentials != null) {
			registeredClients.remove(credentials);
		}
	}

	private Credentials getKeByValue(Object value, Map<Credentials, String> map) {
		Credentials result = null;
		for (Map.Entry<Credentials, String> entry : map.entrySet()){
			if (entry.getValue().equals(value)){
				result = entry.getKey();
				break;
			}
		}
		return result;
	}

	public String getName(String token) {
		return getKeByValue(token, registeredClients).login;
	}
}
