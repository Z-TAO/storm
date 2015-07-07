package backtype.storm.sharedcontext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by tao on 7/5/15.
 */
public class Client{
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);


    public final static int EPHEMERAL = 1;
    public final static int PERSISTENT = 2;
    public final static int SEQUENTIAL = 3;

    public ContextListener cl;
    public ShareContext sc;
    public ArrayList<String> ephemeralNodeList;

    //private static Lock lock = new ReentrantLock();
    public String root;
    private String sessionId;


    public Client(ContextListener cl){
        Init("", new ShareContext(), cl);
    }
    public Client(ShareContext sc, ContextListener cl){
        Init("",sc,cl);
    }
    public Client(String root, ShareContext sc, ContextListener cl){
        Init(root,sc,cl);
    }

    private void Init(String root, ShareContext sc, ContextListener cl){
        this.ephemeralNodeList = new ArrayList<String>();
        this.sc = sc;
        this.cl = cl;
        this.root = root;
        //by everytime init, register this object to the server;
        sessionId = sc.register(this);
    }
    private String getNodeType(int type){
        switch (type){
            case EPHEMERAL:  return "EPHEMERAL";
            case PERSISTENT: return "PERSISTENT";
            case SEQUENTIAL: return "SEQUENTIAL";
        }
        return null;
    }
    public void call(int type, String path){
        cl.method(type, path);
    }

    public String CreateNode(String path, byte[] data, int mode)throws Exception{
        if (mode == EPHEMERAL){
            ephemeralNodeList.add(path);
        }
        path = root + parsePath(path);
        if (mode == SEQUENTIAL){
            //first find the largest num for now
            Integer id = sc.seqTable.containsKey(path)?sc.seqTable.get(path) + 1: 1;
            sc.seqTable.put(path, id);
            path = path + sc.df.format(id);
        }

        LOG.info("Creating node for " + path + " in mode " + getNodeType(mode));
        return sc.createNode(path, data);
    }
    public String parsePath(String path){
        return path.compareTo("/")==0?"":path;
    }

    public void setData(String path, byte [] data)throws Exception{
        path = root + parsePath(path);
        sc.setData(path, data);
    }
    public byte [] getData(String path, boolean watch) throws Exception{
        path = root + parsePath(path);
        return sc.getData(path, watch, sessionId);
    }
    public int getVersion(String path, boolean watch) throws Exception{
        path = root + parsePath(path);
        return sc.getVersion(path, watch, sessionId);
    }
    public String [] getChildren(String path, boolean watch) throws Exception{
        path = root + parsePath(path);
        return sc.getChildren(path, watch, sessionId);
    }
    public boolean Exists(String path, boolean watch) throws Exception{
        path = root + parsePath(path);
        return sc.Exists(path, watch, sessionId);
    }
    public void deleteNode(String path, boolean force) throws Exception{
        path = root + parsePath(path);
        LOG.info("Delete node " + path);
        sc.deleteNode(path, force);
    }
    public void deleteRecursively(String path, boolean force) throws Exception{
        path = root + parsePath(path);
        LOG.info("Delete children from node " + path);
        sc.deleteAll(path, force);
    }

    public void close() throws Exception{
        LOG.info("client closed.");

        sc.unregister(sessionId);
        for (String path : ephemeralNodeList) {
            deleteNode(path, true);
        }
        ephemeralNodeList.clear();
    }

    public static class callback implements ContextListener{
        @Override
        public void method(int type, String path) {
            LOG.warn("get " + type + " at:" + path);

        }
    }
    public static void main(String[] args) throws Exception {

        ShareContext.init();
        Client client = new Client( new ShareContext(), new callback());
        System.out.println(client.Exists("/happy/1/2/3", false));

        System.out.println(client.CreateNode("/happy", null, PERSISTENT));
        System.out.println(client.CreateNode("/happy/1", null, PERSISTENT));
        System.out.println(client.CreateNode("/happy/1/2", null, PERSISTENT));
        System.out.println(client.CreateNode("/happy/1/2/3", null, PERSISTENT));
        String [] s = client.getChildren("/", true);
        client.setData("/happy", new byte[]{1, 2, 3});
        System.out.println(client.Exists("/happy/1/2/3", false));
        byte [] x = client.getData("/happy/1/2/3", false);
        byte [] y = client.getData("/happy", false);
        int v1 = client.getVersion("/happy", false);
        v1 = client.getVersion("/happy/1", false);
        v1 = client.getVersion("/happy/1/2", false);
        v1 = client.getVersion("/happy/1/2/3", false);
        client.deleteNode("kj/jkgh/hki", true);
        client.deleteNode("/happy/1", false);
        boolean f = client.Exists("/happy", false);
        f = client.Exists("/happy/1", false);
        f = client.Exists("/happy", false);


        String[] c = client.getChildren("/", false);

        client.CreateNode("/a", null, SEQUENTIAL);
        client.CreateNode("/a", null, SEQUENTIAL);
        client.CreateNode("/a", null, SEQUENTIAL);
        client.CreateNode("/a", null, SEQUENTIAL);
        client.close();

        client = new Client( new ShareContext(), new callback());
        f = client.Exists("/a", false);
        //byte [] data = client.getData("/a", false);

        //client.deleteNode("/happy/1",false);
    }
}

