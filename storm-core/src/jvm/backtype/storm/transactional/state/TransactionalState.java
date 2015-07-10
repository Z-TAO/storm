/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.transactional.state;

import backtype.storm.Config;
import backtype.storm.serialization.KryoValuesDeserializer;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.sharedcontext.Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TransactionalState {
    Client _curator;
    KryoValuesSerializer _ser;
    KryoValuesDeserializer _des;

    public static TransactionalState newUserState(Map conf, String id, Map componentConf) {
        return new TransactionalState(conf, id, componentConf, "user");
    }

    public static TransactionalState newCoordinatorState(Map conf, String id, Map componentConf) {
        return new TransactionalState(conf, id, componentConf, "coordinator");
    }

    protected TransactionalState(Map conf, String id, Map componentConf, String subroot) {
        try {
            conf = new HashMap(conf);
            // ensure that the serialization registrations are consistent with the declarations in this spout
            if(componentConf!=null) {
                conf.put(Config.TOPOLOGY_KRYO_REGISTER,
                         componentConf
                              .get(Config.TOPOLOGY_KRYO_REGISTER));
            }
            String rootDir = conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT) + "/" + id + "/" + subroot;
            //<String> servers = (List<String>) getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Config.STORM_ZOOKEEPER_SERVERS);
            //Object port = getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_PORT, Config.STORM_ZOOKEEPER_PORT);
            Client initter = new Client("");
            if (rootDir.lastIndexOf("/") == -1){
                initter.CreateNode(rootDir, null, Client.PERSISTENT);
            }else {
                initter.mkdirs(rootDir.substring(0, rootDir.lastIndexOf("/")));
                initter.CreateNode(rootDir, null, Client.PERSISTENT);
            }


            initter.close();

            //_curator = Utils.newCuratorStarted(conf, servers, port, rootDir);
            _curator = new Client(rootDir);
            _ser = new KryoValuesSerializer(conf);
            _des = new KryoValuesDeserializer(conf);
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }
    
    public void setData(String path, Object obj) {
        path = "/" + path;
        byte[] ser = _ser.serializeObject(obj);
        try {
            if(_curator.Exists(path, false)) {
                _curator.setData(path, ser);
            } else {
                if (path.lastIndexOf("/") != -1){
                    String dir = path.substring(0, path.lastIndexOf("/"));
                    _curator.mkdirs(dir);
                }
                _curator.CreateNode(path, ser, Client.PERSISTENT);
/*
                _curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, ser);
*/
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }        
    }
    
    public void delete(String path) {
        path = "/" + path;
        try {
            _curator.deleteNode(path, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public List<String> list(String path) {
        path = "/" + path;
        try {
            if(!_curator.Exists(path, false)) {
                return new ArrayList<String>();
            } else {
                ArrayList<String> l = new ArrayList<String>();
                String [] children = _curator.getChildren(path, false);
                for (String child : children) {
                    l.add(child);
                }
                return l;
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }   
    }
    
    public void mkdir(String path) {
        setData(path, 7);
    }
    
    public Object getData(String path) {
        path = "/" + path;
        try {
            if(_curator.Exists(path, false)) {
                return _des.deserializeObject(_curator.getData(path, false));
            } else {
                return null;
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void close(){
        _curator.close();
    }
    
    private Object getWithBackup(Map amap, Object primary, Object backup) {
        Object ret = amap.get(primary);
        if(ret==null) return amap.get(backup);
        return ret;
    }
}
