package org.yujoo.baas.storm.test;


import static junit.framework.TestCase.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jmock.Expectations;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.yujoo.baas.storm.click.Fields;
import org.yujoo.baas.storm.click.RepeatVisitBolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

@RunWith(value = Parameterized.class)
public class TestRepeatVisitBold extends StormTestCase {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] { { "192.168.33.100", "Client1", "myintranet.com", "false"  },
                { "192.168.33.100", "Client1", "myintranet.com", "false" } ,
                { "192.168.33.101", "Client2", "myintranet1.com", "true" },
                { "192.168.33.102", "Client3", "myintranet2.com", "false"}};
        return Arrays.asList(data);
    }

    private Jedis jedis;

    private String ip;
    private String clientKey;
    private String url;
    private String expected;

    public TestRepeatVisitBold(String ip, String clientKey, String url, String expected){
        this.clientKey = clientKey;
        this.ip = ip;
        this.url = url;
        this.expected = expected;
    }

    @BeforeClass
    public static void setupJedis(){
        Jedis jedis = new Jedis("192.168.141.119",6379);
        jedis.auth("123456");
        jedis.flushDB();
        Iterator<Object[]> it = data().iterator();
        while(it.hasNext()){
            Object[] values = it.next();
            if(values[3].equals("false")){
                String key = values[2] + ":" + values[1];
                jedis.set(key, "visited");//unique, meaning it must exist
            }
        }
    }

    @Test
    public void testExecute(){
        jedis = new Jedis("localhost",6379);
        RepeatVisitBolt bolt = new RepeatVisitBolt();
        Map config = new HashMap();
        config.put("redis-host", "localhost");
        config.put("redis-port", "6379");
        final OutputCollector collector = context.mock(OutputCollector.class);
        bolt.prepare(config, null, collector);

        assertEquals(true, bolt.isConnected());

        final Tuple tuple = getTuple();
        context.checking(new Expectations(){{
            oneOf(tuple).getStringByField(Fields.IP);will(returnValue(ip));
            oneOf(tuple).getStringByField(Fields.CLIENT_KEY);will(returnValue(clientKey));
            oneOf(tuple).getStringByField(Fields.URL);will(returnValue(url));
            oneOf(collector).emit(new Values(clientKey, url, expected));
        }});

        bolt.execute(tuple);
        context.assertIsSatisfied();

        if(jedis != null)
            jedis.disconnect();
    }

}
