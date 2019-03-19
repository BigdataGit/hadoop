package org.apache.hadoop.mapreduce.v2.hs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.zookeeperTool.ZookeeperPoolManager;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;



public class HistoryFileInfoToZk {

    private static final Log LOG = LogFactory.getLog(HistoryFileInfoToZk.class);
    private static final String ZK_PATH = "/history";
    private static String LOCAL_HOST_IP = getLocalHostIP();


    private HistoryFileInfoToZk() {
    }

    public static HistoryFileInfoToZk getInstance() {
        HistoryFileInfoToZk historyFileInfoToZk = new HistoryFileInfoToZk();
        return historyFileInfoToZk;
    }

    static HistoryFileInfoToZk getZnodeInstance() {
        return new HistoryFileInfoToZk();
    }

    /**
     * 初始化创建znode节点，上传任务信息
     */
    void uploadJobInfo(ZookeeperPoolManager zookeeperPoolManager, int id, long clusterTimeStamp) {
        ZooKeeper zk = null;
        String znodePath;
        try {
            LOG.info("==Begin to create znode and upload job info==");
            znodePath = ZK_PATH + "/history-" + id + "-" + UUID.randomUUID().toString() + "-" + LOCAL_HOST_IP;
            zk = zookeeperPoolManager.borrowObject();
            createPath(znodePath, zk, id, clusterTimeStamp);
            LOG.info("==Znode create established and job info :[" + id + "]-[" + clusterTimeStamp + "] upload successed==");
        } catch (Exception e) {
            LOG.error("Upload job info error:", e);
        } finally {
            zookeeperPoolManager.returnObject(zk);
        }
    }

    /**
     * Scans the zookeeper path and populates the intermediate cache
     */

    void scanZnodeToIntermediateCache(HistoryFileManager historyFileManager, ZookeeperPoolManager zookeeperPoolManager) {

        LOG.info("Begin to scan the zookeeper path ");
        Map<Integer, Long> keyMap;
        List<String> znodeLists;
        JobId newJobId = null;
        HistoryFileInfo historyFileInfo;
        ZooKeeper zk = null;

        try {
            zk = zookeeperPoolManager.borrowObject();
            //获取zookeeper history根目录中的所有子节点，选择获取非本节点上传的信息s
            znodeLists = getChildren(ZK_PATH, zk);
            if (!CollectionUtils.isEmpty(znodeLists)) {
                for (String znodeName :
                        znodeLists) {
                    if (!znodeName.contains(LOCAL_HOST_IP)) {
                        keyMap = readData(ZK_PATH + "/" + znodeName, zk);
                        if (keyMap != null && keyMap.size() != 0) {
                            LOG.info("Build JobId and populates the intermediate cache");
                            for (Integer key :
                                    keyMap.keySet()) {
                                newJobId = MRBuilderUtils.newJobId(
                                        ApplicationId.newInstance(keyMap.get(key), key), key);
                            }
                            LOG.info("Build the jobId:" + String.valueOf(newJobId));
                            historyFileInfo = historyFileManager.getFileInfo(newJobId);

                            if (historyFileInfo != null) {
                                historyFileManager.jobListCache.addIfAbsent(historyFileInfo);
                                LOG.info("Populates the intermediate cache completed!");
                            } else {
                                LOG.error("Get job fileinfo is null");
                            }
                            deleteNode(ZK_PATH + "/" + znodeName, zk);
                        } else {
                            LOG.info("Get job info is null ,begin to delete znode");
                        }
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("Scans the zookeeper path or populates the intermediate cache error", e);
        } finally {
            zookeeperPoolManager.returnObject(zk);
        }
    }


    /**
     * 异步创建节点
     *
     * @param path 节点path
     */
    private void createPath(String path, ZooKeeper zooKeeper, int id, long clusterTimeStamp) {
        LOG.info("Asnyc create znode processing");
        Map<Integer, Long> keyMap = new HashMap<>();
        try {
            keyMap.put(Integer.valueOf(id), Long.valueOf(clusterTimeStamp));
            zooKeeper.create(path, objectToByteArray(keyMap), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new IStringCallBack(), "context");
            Thread.sleep(2000);
        } catch (Exception e) {
            LOG.error("ZK node create false, Path: " + path);
        }
    }

    static class IStringCallBack implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int rc, String path, Object ctx,
                String name) {
            LOG.info("Create path result " + rc + " " + path + " " + ctx + " " + name);
        }
    }

    /**
     * 读取指定节点数据内容
     *
     * @param path 节点path
     */
    private HashMap<Integer, Long> readData(String path, ZooKeeper zooKeeper) {

        try {
            LOG.info("Get data success,path:" + path);
            return (HashMap<Integer, Long>) byteArrayToObject(zooKeeper.getData(path, false, null));
        } catch (KeeperException e) {
            LOG.error("Get data KeeperException，path: " + path, e);
        } catch (InterruptedException e) {
            LOG.error("Get data InterruptedException，path: " + path, e);
        }
        return null;
    }

    /**
     * 更新指定节点数据内容
     *
     * @param path 节点path
     * @param fileInfo 数据内容
     */
    private boolean writeData(String path, HistoryFileInfo fileInfo, ZooKeeper zooKeeper) {
        try {
            LOG.info("更新数据成功，path：" + path + ", stat: " +
                    zooKeeper.setData(path, objectToByteArray(fileInfo), -1));
            return true;
        } catch (KeeperException e) {
            LOG.error("更新数据失败，发生KeeperException，path: " + path, e);
        } catch (InterruptedException e) {
            LOG.debug("更新数据失败，发生 InterruptedException，path: " + path, e);
        }
        return false;
    }

    /**
     * 获取子节点列表，在子节点列表变更时触发(只拉取其他节点上传的数据)
     */
    private List<String> getChildren(String rootPath, ZooKeeper zooKeeper) {
        List<String> childList = new ArrayList<>();
        try {
            LOG.info("Get Children list from the path:/history");
            childList = zooKeeper.getChildren(rootPath, new Watcher() {
                /**这个znode的子节点变化的时候会收到通知,这里可以保持长连接，如果子节点变化，
                 * 回调执行加载cache，但是这里会监控子节点的删除，删除节点并不想触发加载cache
                 * */
                @Override
                public void process(WatchedEvent watchedEvent) {
                    LOG.info("Trigger event " + watchedEvent);
                }
            });
            return childList;
        } catch (Exception e) {
            LOG.error("Get children znode error:", e);
            return childList;
        }
    }

    /**
     * 删除指定节点
     *
     * @param path 节点path
     */
    private void deleteNode(String path, ZooKeeper zooKeeper) {
        try {
            zooKeeper.delete(path, -1);
            LOG.info("Delete znode path:" + path + " success");
        } catch (KeeperException e) {
            LOG.error("Delete znode false,KeeperException，path: " + path, e);
        } catch (InterruptedException e) {
            LOG.error("Delete znode false,InterruptedException，path: " + path, e);
        }
    }

    /**
     * 字节数组转为对象
     */
    private static Object byteArrayToObject(byte[] bytes) {
        Object obj = null;
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            obj = objectInputStream.readObject();
        } catch (Exception e) {
            LOG.error("byteArrayToObject failed,:" + e);
        } finally {
            if (byteArrayInputStream != null) {
                try {
                    byteArrayInputStream.close();
                } catch (IOException e) {
                    LOG.error("Close byteArrayInputStream failed, " + e);
                }
            }
            if (objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    LOG.error("Close objectInputStream failed, " + e);
                }
            }
        }
        return obj;
    }

    /**
     * 对象转Byte数组
     */
    private static byte[] objectToByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(obj);
            objectOutputStream.flush();
            bytes = byteArrayOutputStream.toByteArray();

        } catch (IOException e) {
            LOG.error("objectToByteArray failed, " + e);
        } finally {
            if (objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    LOG.error("close objectOutputStream failed, " + e);
                }
            }
            if (byteArrayOutputStream != null) {
                try {
                    byteArrayOutputStream.close();
                } catch (IOException e) {
                    LOG.error("close byteArrayOutputStream failed, " + e);
                }
            }

        }
        return bytes;
    }

    private static String getLocalHostIP() {
        String ip = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();
        } catch (Exception ex) {
            LOG.error("Get local IP error", ex);
        }
        return ip;
    }

    public static void main(String[] args) {
        ZookeeperPoolManager zookeeperPoolManager = ZookeeperPoolManager.getInstance();
        ZooKeeper zk = zookeeperPoolManager.borrowObject();
        HistoryFileInfoToZk h = HistoryFileInfoToZk.getInstance();
//        h.readData("/history/history-138-af4e878d-62df-4f86-b1c2-ec2adcbd721a-10.202.106.84",zk);
        List<String> list = h.getChildren("/history", zk);
        for (String no :
                list) {
            h.deleteNode("/history/" + no, zk);
        }

    }

}

