
package org.apache.hadoop.mapreduce.v2.hs.zookeeperTool;

import java.util.NoSuchElementException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZK实例池管理器
 */
public class ZookeeperPoolManager {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperPoolManager.class);

    private ObjectPool pool;

    /**
     * 构造方法
     */
    private ZookeeperPoolManager() {

        PoolableObjectFactory factory = new ZookeeperPoolableObjectFactory();

        // 初始化ZK对象池
        int maxIdle = NumberUtils.toInt(Configuration.MAX_IDLE);
        int initIdleCapacity = NumberUtils.toInt(Configuration.INIT_IDLE_CAPACITY);
        //构建对象池
        pool = new StackObjectPool(factory, maxIdle, initIdleCapacity);
        // 初始化
        for (int i = 0; i < initIdleCapacity; i++) {
            try {
                pool.addObject();
            } catch (IllegalStateException ex) {
                log.error("The Zookeeper connection pool initialization is abnormal.", ex);
            } catch (UnsupportedOperationException ex) {
                log.error("The Zookeeper connection pool initialization is abnormal.", ex);
            } catch (Exception ex) {
                log.error("The Zookeeper connection pool initialization is abnormal.", ex);
            }
        }
        if (log.isInfoEnabled()) {
            log.info("Zookeeper connection pool initialization completed");
        }
    }

    /**
     * 静态内部类构建单例
     */
    private static class SingletonZK {

        private static final ZookeeperPoolManager ZOOKEEPER_POOL_MANAGER = new ZookeeperPoolManager();
    }

    /**
     * 返回单例的对象
     */
    public static ZookeeperPoolManager getInstance() {

        return SingletonZK.ZOOKEEPER_POOL_MANAGER;
    }

    /**
     * 将ZK对象从对象池中取出
     */
    public ZooKeeper borrowObject() {

        if (pool != null) {
            try {
                if (pool.getNumActive() > 40) {
                    log.warn("超出连接数");
                    return null;
                }
                ZooKeeper zk = (ZooKeeper) pool.borrowObject();
                if (log.isDebugEnabled()) {
                    log.debug("Return the connection from the Zookeeper connection pool，zk.sessionId=" + zk.getSessionId());
                }
                return zk;
            } catch (NoSuchElementException ex) {
                log.error("An exception occurred while getting the connection from the Zookeeper connection pool：", ex);
            } catch (IllegalStateException ex) {
                log.error("An exception occurred while getting the connection from the Zookeeper connection pool：", ex);
            } catch (Exception e) {
                log.error("An exception occurred while getting the connection from the Zookeeper connection pool：", e);
            }
        }
        return null;
    }

    /**
     * 将ZK实例返回对象池
     */
    public void returnObject(ZooKeeper zk) {

        if (pool != null && zk != null) {
            try {
                pool.returnObject(zk);
                if (log.isDebugEnabled()) {
                    log.debug("Return the connection to the zoo administrator connection pool，zk.sessionId=" + zk.getSessionId());
                }
            } catch (Exception ex) {
                log.error("An exception occurred while returning the connection to the Zookeeper connection pool：", ex);
            }
        }
    }

    /**
     * 关闭对象池
     */
    public void close() {
        if (pool != null) {
            try {
                pool.close();
                if (log.isInfoEnabled()) {
                    log.info("Close the Zookeeper connection pool to complete");
                }
            } catch (Exception ex) {
                log.error("An exception occurred while closing the Zookeeper connection pool：", ex);
            }
        }
    }
    
}
