(ns onyx.plugin.rabbitmq-input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.rabbitmq-input :as rmq-plugin]
            [onyx.plugin.rabbit :as rmq]
            [clojure.core.async :refer [>!!]]
            [environ.core :refer [env]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(taoensso.timbre/set-level! :debug)

(def env-config
  {:onyx/id id
   :zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config
  {:onyx/id id
   :zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port-range [40200 40260]
   :onyx.messaging/bind-addr "localhost"})

(def test-env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 100)

(def batch-size 20)

(def test-queue-name (or (env :test-queue-name) "test.queue"))

(def rabbitmq-host (or (env :rabbitmq-host) "localhost"))

(def rabbitmq-port (or (env :rabbitmq-port) "5671"))

(def rabbitmq-key (or (env :rabbitmq-key) nil))

(def rabbitmq-crt (or (env :rabbitmq-crt) nil))

(def rabbitmq-ca-crt (or (env :rabbitmq-ca-crt) nil))

(def rabbitmq-serializer (or (env :rabbitmq-serializer) :onyx.plugin.rabbit/edn-serializer))

(def rabbitmq-deserializer (or (env :rabbitmq-deserializer) :onyx.plugin.rabbit/edn-deserializer))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.rabbitmq-input/input
    :onyx/type :input
    :onyx/medium :rabbitmq
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :rabbit/queue-name test-queue-name
    :rabbit/host rabbitmq-host
    :rabbit/port rabbitmq-port
    :rabbit/key rabbitmq-key
    :rabbit/crt rabbitmq-crt
    :rabbit/ca-crt rabbitmq-ca-crt
    :rabbit/deserializer rabbitmq-deserializer
    :onyx/doc "Read segments from rabbitmq queue"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:in :out]])

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.rabbitmq-input/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(let [serializer (rmq-plugin/resolve-keyword rabbitmq-serializer)
      ctx (rmq/start-publisher (rmq-plugin/task-map->rabbit-params (first catalog)) serializer)
      ch (:write-ch ctx)]
  (doseq [n (range n-messages)]
    (>!! ch {:n n}))
  (>!! ch :done)
  (rmq/stop ctx))

(def v-peers (onyx.api/start-peers 5 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(deftest testing-output
  (testing "Input is received at output"
    (let [expected (set (map (fn [x] {:n x}) (range n-messages)))]
      (is (= expected (set (butlast results))))
      (is (= :done (last results))))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env test-env)
