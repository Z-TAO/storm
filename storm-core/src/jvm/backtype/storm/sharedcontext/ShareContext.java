package backtype.storm.sharedcontext;


import backtype.storm.serialization.types.ArrayListSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.DecimalFormat;
import java.util.ArrayList;
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
    public static final DecimalFormat df = new DecimalFormat("00000000");
    public static Hashtable<String, Integer> seqTable = new Hashtable<String, Integer>();

    private static Hashtable<String, Client> clientLists = new Hashtable<String,Client>();
    private static Hashtable<String, HashSet<String>> watchedPaths = new Hashtable<String, HashSet<String>>();


    private static int NONE = -1;
    private static int NODECREATED = 1;
    private static int NODEDELETED = 2;
    private static int NODEDATACHANGED = 3;
    private static int NODECHILDRENCHANGED = 4;

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
    }

    public static void init(){
        if (root == null){
            root = new Node("/", null, null);
            LOG.info("Share context created.");
        }
    }

    public String register(Client c){
        String u =UUID.randomUUID().toString();
        clientLists.put(u, c);
        return u;
    }
    public void unregister(String id){
        clientLists.remove(id);
    }
    synchronized void callback(String path, int mode){
        String tmp;
        if (watchedPaths.containsKey(path)){
            for (String session : watchedPaths.get(path)) {
                try {
                    Client c = clientLists.get(session);
                    tmp = path.substring(c.root.length());
                    if (tmp.length() == 0){
                        c.call(mode, "/");
                    }else{
                        c.call(mode, tmp);
                    }
                }catch (NullPointerException exp ){
                    LOG.warn("client "+session+" already disconnected.");
                }
            }
            watchedPaths.remove(path);
        }
    }

    synchronized public String createNode(String path, byte [] data) throws Exception{
        int loc = path.lastIndexOf("/");
        String existPath = path.substring(0, loc + 1);
        String name = path.substring(loc+1);
        Node e = findNode(existPath, false);
        if (e.children.containsKey(name)){
            throw new java.lang.IllegalArgumentException("The path already existed.");
        }
        Node node = new Node(name, data, e);
        e.children.put(name, node);
        //TODO: check watch
        callback(path, NODECREATED);
        callback(path.substring(0,path.lastIndexOf("/")), NODECHILDRENCHANGED);
        return name;
    }
    synchronized public void setData(String path, byte [] data) throws Exception{
        Node e = findNode(path, false);
        e.data = data;
        e.version ++;
        //TODO: check watch
        callback(path, NODEDATACHANGED);
        callback(path.substring(0,path.lastIndexOf("/")), NODECHILDRENCHANGED);
    }
    synchronized void deleteNode(String path, boolean force) throws Exception{
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

    private void addWatchCallbacks(String path, String sessionId){
        if (watchedPaths.containsKey(path)){
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
    public int getVersion(String path, boolean watch, String sessionId) throws Exception{
        if (watch){
            addWatchCallbacks(path, sessionId);
        }
        Node e = findNode(path, true);
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
    public static void shutDown(){
        LOG.info("zookeeper shut down.");
        root.name = "/";
        root.data = null;
        root.children.clear();
        root.version = 0;
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
