package backtype.storm.sharedcontext;
import backtype.storm.messaging.netty.Context;
import backtype.storm.sharedcontext.ShareContext.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.dc.path.PathException;

/**
 * Created by tao on 7/5/15.
 */
public class Client{
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    public int NONE = -1;
    public int NODECREATED = 1;
    public int NODEDELETED = 2;
    public int NODEDATACHANGED = 3;
    //public int NODECHILDRENCHANGED = 4;
    public ContextListener cl;
    public ShareContext sc;

    public Client(ContextListener cl){
        sc = new ShareContext();
        this.cl = cl;
    }
    public Client(ShareContext sc, ContextListener cl){
        this.sc = sc;
        this.cl = cl;
    }
    public void call(int type, String path){
        cl.method(type, path);
    }

    public String CreateNode(String path, byte[] data)throws Exception{
        LOG.info("Creating node for " + path);
        //call(NODECREATED, path);
        return sc.createNode(path, data);
    }
    public void setData(String path, byte [] data, boolean watch)throws Exception{
        if (watch) {
            call(NODEDATACHANGED, path);
        }
        sc.setData(path, data);
    }
    public byte [] getData(String path, boolean watch) throws Exception{
        if (watch){
            call(NONE, path);
        }
        return sc.getData(path);
    }
    public int getVersion(String path, boolean watch) throws Exception{
        if (watch){
            call(NONE, path);
        }
        return sc.getVersion(path);
    }
    public boolean Exists(String path, boolean watch) throws Exception{
        if (watch){
            call(NONE, path);
        }
        return sc.Exists(path);
    }
    public void deleteNode(String path, boolean force) throws Exception{
        LOG.info("Delete node " + path);
        sc.deleteNode(path, force);
        call(NODEDELETED, path);
    }
    public void getChildren(String path, boolean watch) throws Exception{

    }
    public void close(){
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

        System.out.println(client.CreateNode("/happy", null));
        System.out.println(client.CreateNode("/happy/1", null));
        System.out.println(client.CreateNode("/happy/1/2", null));
        System.out.println(client.CreateNode("/happy/1/2/3", null));
        client.setData("/happy", new byte[]{1, 2, 3}, false);
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
        //client.deleteNode("/happy/1",false);
    }
}

