package org.yujoo.baas.storm.click;

import org.json.simple.JSONObject;

/**
 * Created with IntelliJ IDEA.
 * User: admin
 * Date: 2012/12/07
 * Time: 5:29 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IPResolver {

    public JSONObject resolveIP(String ip);
}
