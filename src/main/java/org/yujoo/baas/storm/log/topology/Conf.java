package org.yujoo.baas.storm.log.topology;


public class Conf {
    public static final String REDIS_HOST_KEY = "redisHost";
    public static final String REDIS_PORT_KEY = "redisPort";
    public static final String ELASTIC_CLUSTER_NAME = "ElasticClusterName";
    public static final String DEFAULT_ELASTIC_CLUSTER = "LogStorm";
    
    public static final String COUNT_CF_NAME = "LogVolumeByMinute";
    public static final String LOGGING_KEYSPACE = "Logging";
    
    public static final String DEFAULT_JEDIS_PORT = "6379";
}
