package org.yujoo.baas.storm.test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.yujoo.baas.storm.click.ClickTopology;

import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;

/**
 * The integration test basically injects on the input queue,
 * and then introduces a test bolt which simply persists the 
 * tuple into a JSON object onto an output queue. 
 * Note that test is parameter driven, but the cluster is only
 * shutdown once all tests have run
 * */
@RunWith(value = Parameterized.class)
public class IntegrationTestTopology {
	//input: ip, url, clientID
	//output: ExpectedCountry, ExpectedCountryCount, ExpectedCity, ExpectedCityCount
	@Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] { {new Object[]{ "165.228.250.178", "internal.com",  "Client1"}, //input
        	new Object[]{ "AUSTRALIA", new Long(1), "SYDNEY", new Long(1), new Long(1), new Long(1) } },//expectations
        	{new Object[]{ "165.228.250.178", "internal.com",  "Client1"}, //input
        	new Object[]{ "AUSTRALIA", new Long(2), "SYDNEY", new Long(2), new Long(2), new Long(1) } },
        	{new Object[]{ "183.14.31.58", "internal.com",  "Client1"}, //input, same client, different location
            new Object[]{ "UNITED STATES", new Long(1), "DERRY, NH", new Long(1), new Long(3), new Long(1) } },
            {new Object[]{ "183.14.31.58", "internal.com",  "Client2"}, //input, same client, different location
            new Object[]{ "UNITED STATES", new Long(2), "DERRY, NH", new Long(2), new Long(4), new Long(2) } }};//expectations
        return Arrays.asList(data);
    }

    private static Jedis jedis;
    private static ClickTopology topology = new ClickTopology();
    private static TestBolt testBolt = new TestBolt();

    @BeforeClass
    public static void setup(){
        //We want all output tuples coming to the mock for testing purposes
        topology.getBuilder().setBolt("testBolt",testBolt, 1).globalGrouping("geoStats").globalGrouping("totalStats");
        //run in local mode, but we will shut the cluster down when we are finished
        topology.runLocal(0);
        //jedis required for input and ouput of the cluster
        jedis = new Jedis("192.168.141.119", Integer.parseInt(ClickTopology.DEFAULT_JEDIS_PORT));
        jedis.auth("123456");
        jedis.connect();
        jedis.flushDB();
        //give it some time to startup before running the tests.
        Utils.sleep(5000);
    }

    @AfterClass
    public static void shutDown(){
        topology.shutDownLocal();
        jedis.disconnect();
    }
    
    Object[] input;
    Object[] expected;
    public IntegrationTestTopology(Object[] input,Object[] expected){
    	this.input = input;
    	this.expected = expected;
    }

    @Test
    public void inputOutputClusterTest(){
        JSONObject content = new JSONObject();
        content.put("ip" ,input[0]);
        content.put("url" ,input[1]);
        content.put("clientKey" ,input[2]);

        jedis.rpush("count", content.toJSONString());

        Utils.sleep(3000);
        
        int count = 0;
        String data = jedis.rpop("TestTuple");
        
        while(data != null){
        	JSONArray values = (JSONArray) JSONValue.parse(data);
            
            if(values.get(0).toString().contains("geoStats")){
            	count++;
                assertEquals(expected[0], values.get(1).toString().toUpperCase());
                assertEquals(expected[1], values.get(2));
                assertEquals(expected[2], values.get(3).toString().toUpperCase());
                assertEquals(expected[3], values.get(4));
            } else if(values.get(0).toString().contains("totalStats")) {
            	count++;
                assertEquals(expected[4], values.get(1));
                assertEquals(expected[5], values.get(2));
            } 
            data = jedis.rpop("TestTuple");

        }
        assertEquals(2, count);

    }

}
