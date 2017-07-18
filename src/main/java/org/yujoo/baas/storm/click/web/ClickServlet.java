package org.yujoo.baas.storm.click.web;

import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created with IntelliJ IDEA.
 * User: admin
 * Date: 2012/12/07
 * Time: 8:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class ClickServlet extends HttpServlet
{
    Jedis jedis = new Jedis("192.168.141.119",6379);
    
    @Override
    protected void doGet(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException, IOException
    {
        JSONObject content = new JSONObject();
        content.put("ip" ,httpServletRequest.getParameter("ip"));
        content.put("url" ,httpServletRequest.getParameter("url"));
        content.put("clientKey" ,httpServletRequest.getParameter("clientKey"));
        jedis.auth("123456");
        jedis.rpush("count", content.toJSONString());

        httpServletResponse.setContentType("text/plain");
        PrintWriter out = httpServletResponse.getWriter();
        out.println("Counted!");
        out.close();
    }
}
