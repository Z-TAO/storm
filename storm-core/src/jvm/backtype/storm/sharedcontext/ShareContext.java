package backtype.storm.sharedcontext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.UUID;

/**
 * Created by tao on 7/5/15.
 */

public class ShareContext {
    //this is the tree structure
    private static final Logger LOG = LoggerFactory.getLogger(ShareContext.class);
    public static Node root = null;
    public static DecimalFormat df = null;
    public static Hashtable<String, Integer> seqTable = null;

    public static Hashtable<String, ContextListener> clientLists = null;
    private static Hashtable<String, HashSet<String>> watchedPaths = null;
    public static Hashtable<String, String> clientRoots = null;


    private static int NONE;
    private static int NODECREATED;
    private static int NODEDELETED;
    private static int NODEDATACHANGED;
    private static int NODECHILDRENCHANGED;

    static{
        NONE = -1;
        NODECREATED = 1;
        NODEDELETED = 2;
        NODEDATACHANGED = 3;
        NODECHILDRENCHANGED = 4;
    }

    public static class Node{
        public byte [] data;
        public String name;
        public Hashtable<String, Node> children;
        public Node parent;
        public int version; //every update, inc.

        public void init(String name, byte[] data, Node parent){
            this.data = data;
            this.children = new Hashtable<String, Node>();
            this.version = 0;
            this.name = name;
            this.parent = parent;

        }

        public Node(String name, byte[] data, Node parent){
            init(name,data,parent);
        }
    }

    public ShareContext(){
        if (root == null){
            root = new Node("/", null, null);
        }
        if (df ==null){
            df = new DecimalFormat("00000000");
        }
        if (seqTable == null){
            seqTable = new Hashtable<String, Integer>();
        }
        if (clientLists == null){
            clientLists = new Hashtable<String,ContextListener>();
        }
        if (watchedPaths == null){
            watchedPaths = new Hashtable<String, HashSet<String>>();
        }
        if (clientRoots == null){
            clientRoots = new Hashtable<String, String>();
        }
    }

    public static void init() throws InterruptedException{
        root = new Node("/", null, null);
        df = new DecimalFormat("00000000");
        seqTable = new Hashtable<String, Integer>();
        clientLists = new Hashtable<String,ContextListener>();
        watchedPaths = new Hashtable<String, HashSet<String>>();
        clientRoots = new Hashtable<String, String>();
    }


    public String createNewId() {
        return UUID.randomUUID().toString();
    }
    public synchronized void unregister(String id){
        if (id == null) return;

        if (clientLists.containsKey(id)){
            clientLists.remove(id);
        }
        if (clientRoots.containsKey(id)){
            clientRoots.remove(id);
        }
    }
    private  void callback(String path, int mode){
        if (watchedPaths.containsKey(path)){
            for (String session : watchedPaths.get(path)) {
                try {
                    ContextListener c = clientLists.get(session);
                    String root = clientRoots.get(session);
                    if (path.length() > root.length()){
                        root = path.substring(root.length());
                    }else if (path.length() == root.length()){
                        root = "/";
                    }
                    c.method(mode, root);
                }catch (NullPointerException exp ){
                    LOG.warn("client "+session+" already disconnected.");
                }
            }
            watchedPaths.remove(path);
        }
    }

     public String createNode(String path, byte [] data) throws Exception{
         int loc = path.lastIndexOf("/");
         String existPath = path.substring(0, loc + 1);
         String name = path.substring(loc+1);
         Node e = findNode(existPath, false);
         if (e.children.containsKey(name)){
            // throw new java.lang.IllegalArgumentException("The path already existed.");
             return name;
         }
         Node node = new Node(name, data, e);
         e.children.put(name, node);
         //TODO: check watch
         callback(path, NODECREATED);
         if(path.lastIndexOf("/") != -1){
             callback(path.substring(0,path.lastIndexOf("/")), NODECHILDRENCHANGED);
         }
        return name;
    }
     public void setData(String path, byte [] data) throws Exception{
        Node e = findNode(path, false);
        e.data = data;
        e.version ++;
        //TODO: check watch
        callback(path, NODEDATACHANGED);
        callback(path.substring(0,path.lastIndexOf("/")), NODECHILDRENCHANGED);
    }
     void deleteNode(String path, boolean force) throws Exception{
        int loc = path.lastIndexOf("/");
        String last = path.substring(loc + 1);
        String parentPath = path.substring(0,loc);
        Node e = findNode(parentPath, force);
        if (e == null){
            return;
        }else{
            if (e.children.containsKey(last)){
                e.children.remove(last);
                //TODO: check watch
                callback(path, NODEDELETED);
                callback(path.substring(0,path.lastIndexOf("/")), NODECHILDRENCHANGED);
            }else if (force){
                LOG.warn("Path " + path + " not existed.");
                return;
            }else{
                throw new Exception("path not exist!");
            }
        }
    }

     private Node findNode(String path, boolean force) throws Exception{
        Node e = root;
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
                    throw new Exception("path not exist!");
                }
            }
            e = e.children.get(token);
        }
        return e;
    }

     void addWatchCallbacks(String path, String sessionId){
        if (watchedPaths.containsKey(path) && watchedPaths.get(path)!=null){
            watchedPaths.get(path).add(sessionId);
        }else{
            HashSet<String> tmp = new HashSet<String>();
            tmp.add(sessionId);
            watchedPaths.put(path, tmp);
        }
    }
    //TODO: need watch
     public boolean Exists(String path, boolean watch, String sessionId) throws Exception{
        if (watch){
            addWatchCallbacks(path, sessionId);
        }
        if (findNode(path, true) != null){
            return true;
        }else {
            return false;
        }
    }
    //TODO: need watch
     public byte [] getData(String path, boolean watch, String sessionId) throws Exception{
        if (watch){
            addWatchCallbacks(path, sessionId);
        }
        Node e = findNode(path, false);
        if (e.data == null || e.data.length == 0){
            return null;
        }
        return e.data;
    }
    //TODO: need watch
     public Integer getVersion(String path, boolean watch, String sessionId) throws Exception{
        if (watch){
            addWatchCallbacks(path, sessionId);
        }
        Node e = findNode(path, true);
        if (e == null){
            return -1;
        }
        return e.version;
    }
    //TODO: need watch
     public String [] getChildren(String path, boolean watch, String sessionId) throws Exception{
        if (watch){
            addWatchCallbacks(path, sessionId);
        }
        Node p = findNode(path, true);
        if (p!= null){
            String[] arr = new String[p.children.size()];
            p.children.keySet().toArray(arr);
            return arr;
        }else{
            return new String[0];
        }
    }
     public void deleteAll(String path, boolean force) throws Exception{

        deleteNode(path, force);
        /*Node e = findNode(path, force);
        if (e == null){
            return;
        }else{
            e.children.clear();
        }*/
    }
    public static void shutDown() throws InterruptedException, Exception{
        LOG.info("zookeeper shut down.");
        root = null;
        clientLists = null;
        watchedPaths = null;
        seqTable = null;
        clientRoots = null;
    }

    public static void main(String[] args) throws Exception {
        ShareContext tree = new ShareContext();

        watchedPaths.put("1", new HashSet<String>());
        watchedPaths.get("1").add("1234");
        String xx = watchedPaths.get("1").toString();


        HashSet<String> x = new HashSet<String>();
        x.add("1");
        x.add("1");
    }

}
