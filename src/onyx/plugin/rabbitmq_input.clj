(ns onyx.plugin.rabbitmq-input
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [taoensso.timbre :refer [debug info] :as timbre]
            [onyx.plugin.rabbit :as rmq]
            [clojure.core.async :refer [chan timeout alts!! <!!]]))

(defn input-drained? [pending-messages batch]
  (and (= 1 (count @pending-messages))
       (= (count batch) 1)
       (= (:message (first batch)) :done)))

(defrecord RabbitInput [max-pending batch-size batch-timeout pending-messages delivery-tags drained?]
  p-ext/Pipeline
  ;; Write batch can generally be left as is. It simply takes care of
  ;; Transmitting segments to the next task
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch [_ {:keys [rabbitmq/example-datasource rabbit/in-ch rabbit/context] :as event}]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          ;; Read a batch of up to batch-size from your data-source
          ;; For data-sources which enable read timeouts, please
          ;; make sure to pass batch-timeout into the read call
          timeout-ch (timeout batch-timeout)
          [batch new-tags] (if (pos? max-segments)
                             (loop [segments [] new-delivery-tags [] cnt 0]
                               (if (= cnt max-segments)
                                 [segments new-delivery-tags]
                                 (if-let [message (first (alts!! [in-ch timeout-ch] :priority true))]
                                   (let [message-id (java.util.UUID/randomUUID)]
                                     (recur (conj segments
                                                  (t/input message-id
                                                           (:payload message)))
                                            (conj new-delivery-tags
                                                  {:id message-id
                                                   :delivery-tag (:delivery-tag message)})
                                            (inc cnt)))
                                   [segments new-delivery-tags])))
                             (<!! timeout-ch))]
      ;; Add the read batch to your pending-messages so they can be
      ;; retried later if necessary
      (doseq [m batch t new-tags]
        (swap! pending-messages assoc (:id m) (:message m))
        (swap! delivery-tags assoc (:id t) (:delivery-tag t)))
      ;; Check if you've seen the :done and all the messages have been consumed
      ;; If so, set the drained? atom, which will be returned by drained?
      ;; And also ack the :done message in rabbitmq
      (when (input-drained? pending-messages batch)
        (let [done-segment-id (:id (first batch))]
          (rmq/ack (:ch context) (get @delivery-tags done-segment-id))
          (swap! delivery-tags dissoc done-segment-id)
          (reset! drained? true)))
      {:onyx.core/batch batch}))
  (seal-resource [this event]
    ;; Nothing is required here, however generally most plugins
    ;; have resources (e.g. a connection) to clean up
    )

  p-ext/PipelineInput
  (ack-segment [_ {:keys [rabbit/context]} segment-id]
    ;; When a message is fully acked you can remove it from pending-messages
    ;; Generally this can be left as is.
    (rmq/ack (:ch context) (get @delivery-tags segment-id))
    (swap! delivery-tags dissoc segment-id)
    (swap! pending-messages dissoc segment-id))

  (retry-segment
    [_ {:keys [rabbitmq/context] :as event} segment-id]
    ;; Messages are retried if they are not acked in time
    ;; or if a message is forcibly retried by flow conditions.
    ;; Generally this takes place in two steps
    ;; Take the message out of your pending-messages atom, and put it
    ;; back into a datasource or a buffer that are you are reading into
    (when-let [msg (get @pending-messages segment-id)]
      (rmq/requeue (:ch context) (get @delivery-tags segment-id))
      (swap! pending-messages dissoc segment-id)
      (swap! delivery-tags dissoc segment-id)))

  (pending?
    [_ _ segment-id]
    ;; Lookup a message in your pending messages map.
    ;; Generally this can be left as is
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    ;; Return whether the input has been drained. This is set in the read-batch
    @drained?))

;; Builder function for your plugin. Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn input [event]
  (let [task-map (:onyx.core/task-map event)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        pending-messages (atom {})
        delivery-tags (atom {})
        drained? (atom false)]
    (->RabbitInput max-pending batch-size batch-timeout pending-messages delivery-tags drained?)))

(defn task-map->rabbit-params
  [{:keys [rabbit/queue-name rabbit/host rabbit/port rabbit/key rabbit/crt rabbit/ca-crt]}]
  {:queue-name queue-name
   :host host :port port
   :key key :crt crt :ca-crt ca-crt})

(defn resolve-keyword
  [kw]
  (let [namespace (symbol (namespace kw))
        f (symbol (name kw))]
    (ns-resolve namespace f)))

(defn start-rabbit-consumer
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (let [ch (chan 1000)
        deserializer (resolve-keyword (:rabbit/deserializer task-map))
        ctx (rmq/start-consumer (task-map->rabbit-params task-map) deserializer ch)
        ]
    {:rabbit/in-ch ch
     :rabbit/context ctx}))

(defn stop-rabbit-connection
  [{:keys [rabbit/context] :as event} lifecycle]
  (rmq/stop context))

(def reader-calls
  {:lifecycle/before-task-start start-rabbit-consumer
   :lifecycle/after-task-stop stop-rabbit-connection})
