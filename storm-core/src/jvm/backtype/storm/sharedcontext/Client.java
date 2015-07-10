package backtype.storm.sharedcontext;
import backtype.storm.messaging.netty.Context;
import backtype.storm.serialization.types.HashMapSerializer;
import backtype.storm.serialization.types.HashSetSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;
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
    private final static int NONE = -1;
    private final static int NODECREATED = 1;
    private final static int NODEDELETED = 2;
    private final static int NODEDATACHANGED = 3;
    private final static int NODECHILDRENCHANGED = 4;



    public static class Node{
        public Hashtable<String, Node> children;
        //public String name;//useless
        public Integer version;
        public byte [] data;
        public Integer incNumber;

        public Node(byte [] data){
            this.data = data;
            this.version = 0;
            children = new Hashtable<String, Node>();
            incNumber = 0;
        }

    }

    //for all the clients
    private static Node rootNode;
    private static Hashtable<String, ContextListener> clientCallbackLists;
    private static Hashtable<String, HashSet<String>> clientCallbackSessions;
    static {
        rootNode = new Node(null);
        clientCallbackLists = new Hashtable<String, ContextListener>();
        clientCallbackSessions = new Hashtable<String, HashSet<String>>(); //path --> clients sesionIds
    }


    //for one single client;
    private String sessionId;
    private String rootDir;
    private DecimalFormat df;
    private Vector<String> ephemeralList;


    public Client(String rootDir) throws InterruptedException{
        sessionId = generateSessionId();
        this.rootDir = rootDir;
        df = new DecimalFormat("00000000");
        ephemeralList = new Vector<String>();
    }

    public void addListener(ContextListener cl) throws InterruptedException{
        clientCallbackLists.put(sessionId, cl);
    }
    private String generateSessionId(){
        return UUID.randomUUID().toString();
    }

    public void close(){
        //TODO:clear ephemeralNodeList
        LOG.info("client "+sessionId+" closed.");
        for (String s : ephemeralList) {
            deleteNode(s, true);
        }
        clientCallbackLists.remove(sessionId);
        ephemeralList.clear();
    }

    public static void shutdown(){
        rootNode = new Node(null);
        clientCallbackLists = new Hashtable<String, ContextListener>();
        clientCallbackSessions = new Hashtable<String, HashSet<String>>();
    }

    public static String getNodeType(int type){
        switch (type){
            case EPHEMERAL:  return "EPHEMERAL";
            case PERSISTENT: return "PERSISTENT";
            case SEQUENTIAL: return "SEQUENTIAL";
        }
        return null;
    }
    public static String getActionType(int type){
        switch (type){
            case NONE : return "NONE";
            case NODECREATED : return "NODECREATED";
            case NODEDELETED : return "NODEDELETED";
            case NODEDATACHANGED : return "NODEDATACHANGED";
            case NODECHILDRENCHANGED : return "NODECHILDRENCHANGED";
        }
        return null;
    }

    public String parsePath(String path){
        return path.compareTo("/")==0?"":path;
    }

    public Node findNode(String path, boolean force) throws RuntimeException{
        Node e = rootNode;
        String [] tokens = path.split("/");
        for (String token : tokens){
            if (token.isEmpty()){
                continue;
            }
            if (!e.children.containsKey(token)){
                if (force){
                    //LOG.warn("Path "+path+" not existed.");
                    return null;
                }else{
                    throw new RuntimeException("path not exist!");
                }
            }
            e = e.children.get(token);
        }
        return e;
    }

    private String getParentPath(String path){
        //if just "/", don't have one
        if (path.compareTo("/") == 0){
            return "";
        }
        //now they are like "/a...
        return path.substring(0, path.lastIndexOf("/"));
    }

    private void callback(int type, String path){


        String childPath = path.substring(rootDir.length());
        childPath = childPath.compareTo("")==0?"/":childPath;
        if (clientCallbackSessions.containsKey(path)){
            for (String session : clientCallbackSessions.get(path)) {
                if (clientCallbackLists.containsKey(session)){
                    clientCallbackLists.get(session).method(type, childPath);
                }
            }
            clientCallbackSessions.remove(path);
        }
    }

    private void setCallback(String path, String sessionId){
        if (clientCallbackSessions.containsKey(path)){
            clientCallbackSessions.get(path).add(sessionId);
        }else{
            HashSet<String> s = new HashSet<String>();
            s.add(sessionId);
            clientCallbackSessions.put(path, s);
        }
    }
    public String CreateNode(String path, byte[] data, int mode) throws RuntimeException{
        LOG.info("Creating node for " + path + " in mode " + getNodeType(mode));
        if (mode == EPHEMERAL){
            ephemeralList.add(path);
        }
        String realPath = rootDir + parsePath(path);
        //STEP 1: find the node;
        String parentPath = getParentPath(realPath);
        Node e = findNode(parentPath, false); //if not exists, throw exception
        String addedPath = realPath.substring(parentPath.length() + 1);
        if (mode == SEQUENTIAL){
            synchronized (this) {
                addedPath += df.format(e.incNumber);
                e.incNumber++;
            }
        }
        //add
        Node addedNode = new Node(data);
        e.children.put(addedPath, addedNode);

        callback(NODECREATED, realPath);
        //parents
        callback(NODECHILDRENCHANGED, getParentPath(realPath));

        return path;
    }

    public void deleteNode(String path, boolean force) throws RuntimeException{
        String realPath = rootDir + parsePath(path);
        LOG.info("Delete node " + path);
        String parentPath = getParentPath(realPath);
        String childPath = realPath.substring(parentPath.length()+1);
        Node e = findNode(parentPath, force);
        if (e != null && e.children.containsKey(childPath)){
            callback(NODEDELETED, realPath);
            //parents
            callback(NODECHILDRENCHANGED, getParentPath(realPath));
            e.children.remove(childPath);
        }
    }

    public void setData(String path, byte [] data)throws RuntimeException{
        LOG.debug("set data on " + path + " with client:" + sessionId);
        String realPath = rootDir + parsePath(path);
        Node e = findNode(realPath, false);
        e.data = data;
        e.version ++;
        callback(NODEDATACHANGED, realPath);
        //parents
        callback(NODECHILDRENCHANGED, getParentPath(realPath));
    }
    public void deleteRecursively(String path, boolean force) throws RuntimeException{
        deleteNode(path, force);
    }

    public byte[] getData(String path, boolean watch) throws RuntimeException{
        LOG.debug("get data on " + path + " with client:" + sessionId + " data:");
        String realPath = rootDir + parsePath(path);
        Node e = findNode(realPath, false);
        if (watch){
           setCallback(realPath, sessionId);
        }
        return e.data;
    }
    public Integer getVersion(String path, boolean watch) throws RuntimeException{
        LOG.debug("get version on " + path + " with client:" + sessionId);
        path = rootDir + parsePath(path);
        Node e = findNode(path, true);
        if (e == null){
            return null;
        }
        if (watch){
            setCallback(path, sessionId);
        }
        return e.version;
    }
    public String [] getChildren(String path, boolean watch) throws RuntimeException{
        LOG.debug("get children on " + path + " with client:" + sessionId);
        path = rootDir + parsePath(path);
        Node e = findNode(path, false);
        if (watch){
            setCallback(path, sessionId);
        }
        return e.children.keySet().toArray(new String[e.children.size()]);
    }

    public boolean Exists(String path, boolean watch) throws RuntimeException{
        path = rootDir + parsePath(path);
        Node e = findNode(path, true);
        if (watch){
            setCallback(path, sessionId);
        }
        return e != null;
    }

    public void mkdirs(String path) throws RuntimeException{
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
            if (tmp.contains("/")){
                tmp = tmp.substring(0, tmp.indexOf("/"));
                checkPath = checkPath + "/" + tmp;
            }else if (tmp.length() !=0){
                checkPath = checkPath + "/" + tmp;
            }

            CreateNode(checkPath, null, PERSISTENT);
        }
    }


    public static class callback implements ContextListener{
        @Override
        public void method(int type, String path) {
            LOG.warn("get " + getActionType(type) + " at:" + path);

        }
    }

  public static void main(String[] args) throws Exception {
        Client cc = new Client("");
      cc.addListener(new callback());
        cc.CreateNode("/happy", null, PERSISTENT);

        Client client = new Client("/happy");
      client.addListener(new callback());
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
        client.deleteNode("/kj/jkgh/hki", true);
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

        client = new Client( "");
      client.addListener(new callback());
        f = client.Exists("/a", false);
        //byte [] data = client.getData("/a", false);

        //client.deleteNode("/happy/1",false);
    }
}

