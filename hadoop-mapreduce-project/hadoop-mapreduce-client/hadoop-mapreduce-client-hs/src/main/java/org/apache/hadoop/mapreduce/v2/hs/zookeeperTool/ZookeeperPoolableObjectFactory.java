package org.apache.hadoop.mapreduce.v2.hs.zookeeperTool;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zookeeper实例对象池，由于一个Zookeeper实例持有一个Socket连接，所以将Zookeeper实例池化避免实例化过程中的消耗
 *
 *
 */
public class ZookeeperPoolableObjectFactory implements PoolableObjectFactory {
    
    private static final Logger log = LoggerFactory.getLogger(ZookeeperPoolableObjectFactory.class);


    @Override
    public ZooKeeper makeObject() throws Exception {
        //返回一个新的zk实例
        ZookeeperConnector zc = new ZookeeperConnector();

        //连接服务端
        String servers = Configuration.SERVERS;
        int timeout = NumberUtils.toInt(Configuration.CONNECTION_TIMEOUT) * 1000;
        ZooKeeper zk = zc.connect(servers, timeout);
        if (zk != null) {
            if (log.isInfoEnabled()) {
                log.info("Instantiate the Zookeeper client object，Zookeeper.sessionId=" + zk.getSessionId());
            }
            return zk;
        } else {
            log.warn("Instantiating a Zookeeper client object failed");
            zc.close();
            throw new Exception("Error in creating connection to ZooKeeper Server: " + servers);
        }
    }


    public void destroyObject(ZooKeeper obj) throws Exception {
        if (obj != null) {
            obj.close();
            if (log.isInfoEnabled()) {
                log.info("Zookeeper client object is closed，Zookeeper.sessionId=" + obj.getSessionId());
            }
        }
    }
    @Override
	public void destroyObject(Object obj) throws Exception {
		destroyObject((ZooKeeper)obj);
	}

    public boolean validateObject(ZooKeeper obj) {

		if (obj != null && obj.getState() == States.CONNECTED) {
            if (log.isDebugEnabled()) {
                log.debug("Zookeeper client object verification failed，Zookeeper.sessionId=" + obj.getSessionId());
            }
            return true;
        }else{
            if (log.isInfoEnabled()) {
                log.info("Zookeeper client object verification failed，Zookeeper.sessionId=" + obj.getSessionId());
            }
            return false;
        }
	}

	@Override
    public boolean validateObject(Object obj) {
		return validateObject((ZooKeeper)obj);
	}


	@Override
	public void activateObject(Object obj) throws Exception {
	}

	@Override
	public void passivateObject(Object obj) throws Exception {
	}
}

