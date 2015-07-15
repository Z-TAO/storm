package backtype.storm.sharedcontext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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


    public Client(ContextListener cl) {
        Init("", new ShareContext(), cl);
    }
    public Client(ShareContext sc, ContextListener cl){
        Init("",sc,cl);
    }
    public Client(String root, ShareContext sc, ContextListener cl){
        Init(root, sc, cl);
    }

    synchronized private void Init(String root, ShareContext sc, ContextListener cl){
        try{
            ephemeralNodeList = new ArrayList<String>();
            this.sc = sc;
            this.cl = cl;
            this.root = root;
            //by everytime init, register this object to the server;
            sessionId = sc.createNewId();
            LOG.info("Client created with id: " + sessionId);
            if (cl !=null){
                sc.clientLists.put(sessionId, cl);
            }
            sc.clientRoots.put(sessionId, root);
            Thread.sleep(10);
        }catch (InterruptedException e){
            LOG.warn("Interrupted");
        }
    }
    synchronized private String getNodeType(int type){
        switch (type){
            case EPHEMERAL:  return "EPHEMERAL";
            case PERSISTENT: return "PERSISTENT";
            case SEQUENTIAL: return "SEQUENTIAL";
        }
        return null;
    }
    synchronized  public void call(int type, String path){
        cl.method(type, path);
    }

    synchronized public String CreateNode(String path, byte[] data, int mode)throws Exception{
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

    synchronized public void setData(String path, byte [] data)throws Exception{
        LOG.debug("set data on " + path + " with client:" + sessionId);
        path = root + parsePath(path);
        sc.setData(path, data);
    }
    synchronized public byte [] getData(String path, boolean watch) throws Exception{
        path = root + parsePath(path);
        byte [] data = sc.getData(path, watch, sessionId);
        LOG.debug("get data on " + path + " with client:" + sessionId + " data:");
        return data;
    }
    synchronized public Integer getVersion(String path, boolean watch) throws Exception{
        LOG.debug("get version on " + path + " with client:" + sessionId);
        path = root + parsePath(path);
        return sc.getVersion(path, watch, sessionId);
    }
    synchronized public String [] getChildren(String path, boolean watch) throws Exception{
        LOG.debug("get children on " + path + " with client:" + sessionId);
        path = root + parsePath(path);
        return sc.getChildren(path, watch, sessionId);
    }
    synchronized public boolean Exists(String path, boolean watch) throws Exception{
        LOG.debug("check exist on " + path + " with client:" + sessionId);
        path = root + parsePath(path);
        return sc.Exists(path, watch, sessionId);
    }
    synchronized public void deleteNode(String path, boolean force) throws Exception{
        path = root + parsePath(path);
        LOG.info("Delete node " + path);
        sc.deleteNode(path, force);
    }
    synchronized public void deleteRecursively(String path, boolean force) throws Exception{
        path = root + parsePath(path);
        LOG.info("Delete children from node " + path);
        sc.deleteAll(path, force);
    }
    synchronized public void mkdirs(String path) throws Exception{

        if (path.compareTo("") == 0){
            return;
        }
        String checkPath = path;
        while (!Exists(checkPath, false) && checkPath.length() != 0){
            int li = checkPath.lastIndexOf("/");
            if (li == -1){
                checkPath = "";
                break;
            }
            checkPath = checkPath.substring(0,li);
        }
        while(checkPath.compareTo(path) !=0){
            String tmp = path.substring(checkPath.length()+1);
            if (tmp.indexOf("/") != -1){
                tmp = tmp.substring(0, tmp.indexOf("/"));
                checkPath = checkPath + "/" + tmp;
            }else if (tmp.length() !=0){
                checkPath = checkPath + "/" + tmp;
            }

            CreateNode(checkPath, null, PERSISTENT);
        }
    }

    public void close(){
        LOG.info("client "+sessionId+" closed.");
        sc.unregister(sessionId);
        for (String path : ephemeralNodeList) {
            try {
                deleteNode(path, true);
            }catch (Exception e){
                //do nothing
            }
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
        Client cc = new Client("", new ShareContext(), new callback());
        cc.CreateNode("/happy", null, PERSISTENT);

        Client client = new Client("/happy", new ShareContext(), new callback());
        System.out.println(client.Exists("/happy/1/2/3", false));

        System.out.println(client.CreateNode("/happy", null, PERSISTENT));
        System.out.println(client.CreateNode("/happy/1", null, PERSISTENT));
        System.out.println(client.CreateNode("/happy/1/2", null, PERSISTENT));
        System.out.println(client.CreateNode("/happy/1/2/3", null, PERSISTENT));
        String [] s = client.getChildren("/", true);
        client.setData("/happy", new byte[]{1, 2, 3});
        System.out.println(client.Exists("/happy/1/2/3", true));
        byte [] x = client.getData("/happy/1/2/3", true);
        byte [] y = client.getData("/happy", true);
        int v1 = client.getVersion("/happy", true);
        v1 = client.getVersion("/happy/1", true);
        v1 = client.getVersion("/happy/1/2", true);
        v1 = client.getVersion("/happy/1/2/3", true);
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

