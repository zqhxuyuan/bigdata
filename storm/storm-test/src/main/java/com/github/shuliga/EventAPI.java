package com.github.shuliga;

import com.github.shuliga.context.AppContext;
import com.github.shuliga.event.command.CommandEvent;
import com.github.shuliga.event.command.CommandJson;
import com.github.shuliga.event.notification.NotificationEvent;
import com.github.shuliga.event.notification.NotificationJson;
import com.github.shuliga.jms.MessageReceiver;
import com.github.shuliga.jms.MessageSender;
import com.github.shuliga.security.Credentials;
import com.github.shuliga.utils.FileUtil;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

/**
 * User: yshuliga
 * Date: 07.01.14 10:53
 */
@Path("event")
@RequestScoped
public class EventAPI {

	@Inject
	AppContext appContext;

	@Inject
	MessageSender sender;

	@Inject
	MessageReceiver receiver;

	@HEAD
	public Response head() {
		return Response.status(Response.Status.OK.getStatusCode()).build();
	}

	@POST
    @Path("command/{token}")
	@Consumes(MediaType.APPLICATION_JSON)
    public void command(CommandJson command, @PathParam("token") String token, @Context HttpServletResponse response) {
		if (assertSecurityToken(response,token) && command != null){
			try {
				System.out.println("Command received: " + command);
				sender.sendCommand(toCommandEvent(command, getNameFromToken(token)));
				ackResponse(response, true);
			} catch (JMSException e) {
				ackResponse(response, false);
			}
		}
    }

	@GET
	@Path("notification")
	@Produces(MediaType.APPLICATION_JSON)
	public NotificationJson getNotification(@QueryParam("token") String token, @Context HttpServletResponse response) {
		NotificationJson notification  = new NotificationJson();
		if (assertSecurityToken(response,token)){
			notification = toNotificationJson(receiver.receiveNotification(getNameFromToken(token)));
			System.out.println("Notification request: " + notification);
		}
		return notification;
	}

	@GET
	@Path("payload/{token}")
	@Produces(MediaType.TEXT_PLAIN)
	public String getNotification(@PathParam("token") String token, @QueryParam("id") String id, @Context HttpServletResponse response) {
		String payload = "";
		if (assertSecurityToken(response,token)){
			System.out.println("Payload request: " + id);
			if (id != null && !id.isEmpty()){
				payload = readFromFile(FileUtil.createJsonPath(FileUtil.PERSISTENCE_ROOT, id, "PharmacyDataStream"));
			}
		}
		return payload;
	}

	private String getNameFromToken(String token) {
		return appContext.getName(token);
	}

	@POST
	@Path("register")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String register(Credentials credentials, @Context HttpServletResponse response) {
		String token  = null;
		System.out.println("Registration request for credentials: " + credentials);
		try {
			token = registerAndGetToken(credentials);
		} catch (JMSException e) {
			token = null;
		}
		assertSecurityToken(response, token);
		return "{\"token\":\"" + token + "\"}";
	}

	@POST
	@Path("logoff/{token}")
	public void logoff(@PathParam("token") String token, @Context HttpServletResponse response) {
		try {
			logoff(token);
			ackResponse(response, true);
		} catch (JMSException e) {
			ackResponse(response, false);
		}
	}

	private String readFromFile(String path) {
		StringBuffer buf = new StringBuffer();
		buf.append(" ");
		Scanner sc = null;
		System.out.println("Reading from file: " + path);
		try {
			sc = new Scanner(new File(path));
			System.out.println("Reading from file: " + path);
			while (sc.hasNext()) {
				buf.append(sc.nextLine());
				buf.append("\n");
			}
		} catch (FileNotFoundException e) {
			System.out.println(" -- IO error reading " + path);
		}
		return buf.toString();
	}

	private boolean assertSecurityToken(HttpServletResponse response, String token) {
		return ackResponse(response, token != null && !token.isEmpty());
	}

	private boolean ackResponse(HttpServletResponse response, boolean ack){
		if (ack){
			response.setStatus(Response.Status.OK.getStatusCode());
		} else {
			response.setStatus(Response.Status.NOT_ACCEPTABLE.getStatusCode());
			System.out.println(" Error sending messages to queue.");
		}
		return ack;
	}

	private String registerAndGetToken(Credentials credentials) throws JMSException {
		String token = appContext.registerClient(credentials);
		sender.sendCommand(new CommandEvent(CommandJson.Action.REGISTER.getLabel(), credentials.login, credentials.role, token, "com.github.shuliga.EventAPI", null));
		return token;
	}

	private void logoff(String token) throws JMSException {
		if (token != null) {
			appContext.logoff(token);
			sender.sendCommand(new CommandEvent(CommandJson.Action.LOGOFF.getLabel(), null, null, token, "com.github.shuliga.EventAPI", null));
		}
	}

	private NotificationJson toNotificationJson(NotificationEvent notificationEvent) {
		return notificationEvent == null ? new NotificationJson() : new NotificationJson(String.valueOf(notificationEvent.eventId), (String)notificationEvent.payload, notificationEvent.sourceId, null);
	}

	private CommandEvent toCommandEvent(CommandJson command, String sourceId) {
		return new CommandEvent(command.action, command.targetName, command.targetRole, command.message, sourceId, command.sourceEventId);
	}

}