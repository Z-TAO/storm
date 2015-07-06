package backtype.storm.sharedcontext;

import backtype.storm.metric.SystemBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.dc.path.PathException;

import java.awt.geom.IllegalPathStateException;
import java.util.Hashtable;

/**
 * Created by tao on 7/5/15.
 */

public class ShareContext {
    //this is the tree structure
    private static final Logger LOG = LoggerFactory.getLogger(ShareContext.class);
    public static Node root = null;


    public static class Node{
        public byte [] data;
        public String name;
        public Hashtable<String, Node> children;
        public int version; //every update, inc.

        public Node(){
            data = null;
            children = new Hashtable<String, Node>();
            version = 0;
            name = null;
        }

        public Node(String name, byte[] data){
            this.data = data;
            children = new Hashtable<String, Node>();
            version = 0;
            this.name = name;
        }
    }

    public ShareContext(){

    }
    public static void init(){
        if (root == null){
            root = new Node("/", null);
            LOG.info("Share context created.");
        }
    }

    public static synchronized String createNode(String path, byte [] data) throws Exception{
        int loc = path.lastIndexOf("/");
        String existPath = path.substring(0, loc + 1);
        String name = path.substring(loc+1);
        Node e = findNode(existPath, false);
        if (e.children.containsKey(name)){
            throw new java.lang.IllegalArgumentException("The path already existed.");
        }
        Node node = new Node(name, data);
        e.children.put(name, node);
        return name;
    }
    private static Node findNode(String path, boolean force) throws Exception{
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
                    throw new PathException("path not exist!");
                }
            }
            e = e.children.get(token);
        }
        return e;
    }
    public static boolean Exists(String path) throws Exception{
        if (findNode(path, true) != null){
            return true;
        }else {
            return false;
        }
    }
    public static synchronized void setData(String path, byte [] data) throws Exception{
        Node e = findNode(path, false);
        e.data = data;
        e.version ++;
    }
    public static byte [] getData(String path) throws Exception{
        Node e = findNode(path, false);
        return e.data;
    }
    public static synchronized int getVersion(String path) throws Exception{
        Node e = findNode(path, false);
        return e.version;
    }
    public static synchronized void deleteNode(String path, boolean force) throws Exception{
        int loc = path.lastIndexOf("/");
        String last = path.substring(loc+1);
        path = path.substring(0,loc);
        Node e = findNode(path, force);

        if (e == null){
            return;
        }else{
            if (e.children.containsKey(last)){
                e.children.remove(last);
            }else if (force){
                LOG.warn("Path " + path + " not existed.");
                return;
            }else{
                throw new PathException("path not exist!");
            }
        }
    }
    public static void main(String[] args) throws Exception {

        ShareContext tree = new ShareContext();
        System.out.println(tree.root.children.isEmpty());

    }

}
