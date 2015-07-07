;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns backtype.storm.zookeeper
  ;(:import [org.apache.curator.retry RetryNTimes])
  ;(:import [org.apache.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener])
  ;(:import [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory])
  ;(:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
  ;          ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
  ;Watcher$Event$EventType KeeperException$NodeExistsException])
  ;(:import [org.apache.zookeeper.data Stat])
  ;(:import [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory])
  ;(:import [java.net InetSocketAddress BindException])
  (:import [java.io File])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.sharedcontext Client ShareContext ContextListener])
  (:use [backtype.storm util log config]))


(def zk-event-types
  {-1 :none
   1 :node-created
   2 :node-deleted
   3 :node-data-changed
   4 :node-children-changed})


(defn- default-watcher
  [state type path]
  (log-message "Zookeeper state update: " state type path))

(defnk mk-client
  [conf servers port
   :root ""
   :watcher default-watcher
   :auth-conf nil]
  (let [client (Client. root (ShareContext.)
                 (reify
                   ContextListener
                   (^void method [this ^int type ^String path]
                     (watcher :connected (zk-event-types type) path))))]
    ;(let [fk (Utils/newCurator conf servers port root (when auth-conf (ZookeeperAuthInfo. auth-conf)))]
    ;;(.. fk
    ;    (getCuratorListenable)
     ;   (addListener
     ;     (reify CuratorListener
     ;       (^void eventReceived [this ^CuratorFramework _fk ^CuratorEvent e]
     ;              (when (= (.getType e) CuratorEventType/WATCHED)
     ;                (let [^WatchedEvent event (.getWatchedEvent e)]
     ;                 (watcher (zk-keeper-states (.getState event))
     ;                           (zk-event-types (.getType event))
     ;                           (.getPath event))))))))
    ;;    (.. fk
    ;;        (getUnhandledErrorListenable)
    ;;        (addListener
    ;;         (reify UnhandledErrorListener
    ;;           (unhandledError [this msg error]
    ;;             (if (or (exception-cause? InterruptedException error)
    ;;                     (exception-cause? java.nio.channels.ClosedByInterruptException error))
    ;;               (do (log-warn-error error "Zookeeper exception " msg)
    ;;                   (let [to-throw (InterruptedException.)]
    ;;                     (.initCause to-throw error)
    ;;                     (throw to-throw)
    ;;                     ))
    ;;               (do (log-error error "Unrecoverable Zookeeper error " msg)
    ;;                   (halt-process! 1 "Unrecoverable Zookeeper error")))
    ;;             ))))
    client))

(def zk-create-modes
  {:ephemeral 1
   :persistent 2
   :sequential 3})

(defn create-node
  ([^Client zk ^String path ^bytes data mode]
   (try
     (.CreateNode zk (normalize-path path) data (zk-create-modes mode))
     (catch Exception e (throw (wrap-in-runtime e)))))
  ([^Client zk ^String path ^bytes data]
   (create-node zk path data :persistent)))

(defn exists-node?
  [^Client zk ^String path watch?]
  (try
    (.Exists zk (normalize-path path) watch?)
    (catch Exception e (throw (wrap-in-runtime e)))))

(defnk delete-node
  [^Client zk ^String path :force false]
  (try-cause  (.deleteNode zk (normalize-path path) force)
             (catch Exception e (throw (wrap-in-runtime e)))))

(defn mkdirs
  [^Client zk ^String path]
  (let [path (normalize-path path)]
    (when-not (or (= path "/") (exists-node? zk path false))
      (mkdirs zk (parent-path path))
      (try-cause
        (create-node zk path (barr 7) :persistent)
        (catch Exception e
          ;; this can happen when multiple clients doing mkdir at same time
          ))
      )))

(defn get-data
  [^Client zk ^String path watch?]
  (let [path (normalize-path path)]
    (try-cause
      (if (exists-node? zk path watch?)
        (.getData zk path watch?)
        nil)
      (catch Exception e (throw (wrap-in-runtime e))))))

(defn get-data-with-version 
  [^Client zk ^String path watch?]
  (try-cause
    (if-let [data
             (if (exists-node? zk path watch?)
                (.getData zk (normalize-path path) watch?))]
      {:data data
       :version (.getVersion zk (normalize-path path) false)})
      (catch Exception e
        ;; this is fine b/c we still have a watch from the successful exists call
        nil )))


(defn get-version
[^Client zk ^String path watch?]
  (if-let [version
           (.getVersion zk (normalize-path path) watch?)]
    version
    nil))

(defn get-children
  [^Client zk ^String path watch?]
  (try
    (into [] (.getChildren zk path watch?))
    (catch Exception e (throw (wrap-in-runtime e)))))

(defn set-data
  [^Client zk ^String path ^bytes data]
  (try
    (.setData zk path data)
    (catch Exception e (throw (wrap-in-runtime e)))))

(defn exists
  [^Client zk ^String path watch?]
  (exists-node? zk path watch?))

(defn delete-recursive
  [^Client zk ^String path]
  (.deleteRecursively zk (normalize-path path) true))


(defnk mk-inprocess-zookeeper
  [localdir :port nil]
  (ShareContext/init)
  [nil nil]
  )

(defn shutdown-inprocess-zookeeper
  [handle]
  (ShareContext/shutDown)
  )
