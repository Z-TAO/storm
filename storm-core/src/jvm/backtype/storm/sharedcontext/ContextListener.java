package backtype.storm.sharedcontext;

/**
 * Created by tao on 7/5/15.
 */
public interface ContextListener {
    void method(int type, String path);
}
