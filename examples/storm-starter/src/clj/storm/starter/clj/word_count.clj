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
(ns storm.starter.clj.word-count
  (:import [backtype.storm StormSubmitter LocalCluster])
  (:use [backtype.storm clojure config])
  (:gen-class))

(def begin-time (System/currentTimeMillis))
(def check-init true)
(def total-counts (atom 0))
(defspout sentence-spout ["sentence"]
  [conf context collector]
  (let [sentences ["a little brown dog"
                   "the man petted the dog"
                   "four score and seven years ago"
                   "an apple a day keeps the doctor away"]]
    (spout
      (nextTuple []
        ;(Thread/sleep 1)
        (if (= check-init true)
          (do
            (locking *out*
              (println "process done in " (/ (- (System/currentTimeMillis) begin-time) 1000.0) "seconds"))
            (System/exit 0)
            ))
        (emit-spout! collector [(rand-nth sentences)])
        )
      (ack [id]
        ;; You only need to define this method for reliable spouts
        ;; (such as one that reads off of a queue like Kestrel)
        ;; This is an unreliable spout, so it does nothing here
        ))))

(defspout sentence-spout-parameterized ["word"] {:params [sentences] :prepare false}
  [collector]
  (Thread/sleep 100)
  (emit-spout! collector [(rand-nth sentences)]))

(defbolt split-sentence ["word"] [tuple collector]
  (let [words (.split (.getString tuple 0) " ")]
    (doseq [w words]
      (emit-bolt! collector [w] :anchor tuple))
    (ack! collector tuple)
    ))

(defbolt word-count ["word" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
      (execute [tuple]
        (let [word (.getString tuple 0)]
          (swap! counts (partial merge-with +) {word 1})
          (swap! total-counts + 1)
          (emit-bolt! collector [word (@counts word)] :anchor tuple)
          (ack! collector tuple)
          (if (= (mod (@counts word) 10000) 0)
            (locking *out*
              (println "the output word" word "reached" (@counts word))))
          (if (> @total-counts 2000000)
            ;(println "the output word" word "reached" (@counts word)))
            (do
              ;(println "the output word" word "reached" (@counts word))
              (locking *out*
                (println "process done in " (/ (- (System/currentTimeMillis) begin-time) 1000.0) "seconds"))
              (System/exit 0)))
          )))))

(defn mk-topology []

  (topology
    {"1" (spout-spec sentence-spout :p 1)
     ;"2" (spout-spec (sentence-spout-parameterized
     ;                  ["the cat jumped over the door"
     ;                   "greetings from a faraway land"])
     ;      :p 1)
     }
    {"3" (bolt-spec {"1" :shuffle ;"2" :shuffle
                     }
           split-sentence
           :p 5)
     "4" (bolt-spec {"3" ["word"]}
           word-count
           :p 6)}))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "word-count" {TOPOLOGY-DEBUG false} (mk-topology))
    (Thread/sleep 100000000)
    (.shutdown cluster)
    ))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
    name
    {TOPOLOGY-DEBUG false
     TOPOLOGY-WORKERS 1}
    (mk-topology)))

(defn -main
  ([]
    (def begin-time (System/currentTimeMillis))
    (run-local!))
  ([name]
    (submit-topology! name)))

(-main)