package org.yujoo.baas.storm.click;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class HttpIPResolver implements IPResolver, Serializable {


    static String url =
            "http://apis.map.qq.com/ws/location/v1/ip";

    @Override
    public JSONObject resolveIP(String ip) {
        URL geoUrl = null;
        BufferedReader in = null;
        try {
            geoUrl = new URL(url + "?ip=" + ip+"&key=56GBZ-FDGWJ-KBYF5-FEX3L-FQECH-W6BCB");
            URLConnection connection = geoUrl.openConnection();
            in = new BufferedReader(
                    new InputStreamReader(
                            connection.getInputStream()));
            String inputLine;

            JSONObject json = (JSONObject) JSONValue.parse(in);

            in.close();

            return json;
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {}
            }
        }
        return null;
    }
}
