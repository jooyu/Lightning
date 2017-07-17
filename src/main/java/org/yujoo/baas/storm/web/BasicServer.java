package org.yujoo.baas.storm.web;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class BasicServer {
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Server server = new Server(8080);
        Context root = new Context(server,"/", Context.SESSIONS);
        root.addServlet(new ServletHolder(new ClickServlet()),"/*");
		server.start();

	}

}
