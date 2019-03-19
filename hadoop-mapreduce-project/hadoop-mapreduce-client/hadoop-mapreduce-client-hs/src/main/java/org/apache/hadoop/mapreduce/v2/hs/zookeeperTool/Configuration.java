
package org.apache.hadoop.mapreduce.v2.hs.zookeeperTool;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 连接池参数配置
 */
public class Configuration {

    private static final Logger log = LoggerFactory.getLogger(Configuration.class);

    /**
     * zk服务器地址
     */
    public static String SERVERS;

    /**
     * 初始连接数
     */
    public static String MAX_IDLE = "20";

    /**
     * zk连接池中，最小的空闲连接数
     */
    public static String INIT_IDLE_CAPACITY = "5";

    /**
     * session的生命周期 单位分钟
     */
    public static String SESSION_TIMEOUT = "5";

    /**
     * 和zk服务器建立的连接的超时时间 单位秒
     */
    public static String CONNECTION_TIMEOUT = "20";


    static {

        try {
            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            configuration.addResource("mapred-site.xml");

            SERVERS = !StringUtils.isEmpty(configuration.get("history.zookeeper.quorum")) ? configuration
                    .get("history.zookeeper.quorum") : SERVERS;
            MAX_IDLE = !StringUtils.isEmpty(configuration.get("zookeeper.session.max_idle")) ? configuration
                    .get("zookeeper.session.max_idle") : MAX_IDLE;
            INIT_IDLE_CAPACITY = !StringUtils.isEmpty(configuration.get("zookeeper.session.init_idle_capacity")) ? configuration
                    .get("zookeeper.session.init_idle_capacity") : INIT_IDLE_CAPACITY;
            SESSION_TIMEOUT = !StringUtils.isEmpty(configuration.get("zookeeper.session.session_timeout")) ? configuration
                    .get("zookeeper.session.session_timeout") : SESSION_TIMEOUT;
            CONNECTION_TIMEOUT = !StringUtils.isEmpty(configuration.get("zookeeper.session.connection_timeout")) ? configuration
                    .get("zookeeper.session.connection_timeout") : CONNECTION_TIMEOUT;
        } catch (Exception e) {
            log.error("Get connection params error:", e);
        }

    }

}
