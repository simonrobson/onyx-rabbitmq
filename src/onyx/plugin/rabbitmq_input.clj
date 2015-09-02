(ns onyx.plugin.rabbitmq-input
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [taoensso.timbre :refer [debug info] :as timbre]))

;; Often you will need some data in your event map for use by the plugin
;; or other lifecycle functions. Try to place these in your builder function (pipeline)
;; first if possible.
(defn inject-into-eventmap
  [event lifecycle]
  (when-not (:rabbitmq/example-datasource event)
    (throw (ex-info ":rabbitmq/example-datasource not found - add it using a :before-task-start lifecycle"
                    {:event-map-keys (keys event)})))

  (let [pipeline (:onyx.core/pipeline event)]
    {:rabbitmq/pending-messages (:pending-messages pipeline)
     :rabbitmq/drained? (:drained? pipeline)
     :rabbitmq/example-datasource (:rabbitmq/example-datasource event)}))

(def reader-calls
  {:lifecycle/before-task-start inject-into-eventmap})

(defn input-drained? [pending-messages batch]
  (and (= 1 (count @pending-messages))
       (= (count batch) 1)
       (= (:message (first batch)) :done)))

(defrecord ExampleInput [max-pending batch-size batch-timeout pending-messages drained?]
  p-ext/Pipeline
  ;; Write batch can generally be left as is. It simply takes care of
  ;; Transmitting segments to the next task
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch [_ {:keys [rabbitmq/example-datasource] :as event}]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          ;; Read a batch of up to batch-size from your data-source
          ;; For data-sources which enable read timeouts, please
          ;; make sure to pass batch-timeout into the read call
          batch (mapv (fn [message]
                        (t/input (java.util.UUID/randomUUID) message))
                      (take batch-size @example-datasource))
          _ (swap! example-datasource (partial drop batch-size))]
      ;; Add the read batch to your pending-messages so they can be
      ;; retried later if necessary
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) (:message m)))
      ;; Check if you've seen the :done and all the messages have been consumed
      ;; If so, set the drained? atom, which will be returned by drained?
      (when (input-drained? pending-messages batch)
        (reset! drained? true))
      {:onyx.core/batch batch}))
  (seal-resource [this event]
    ;; Nothing is required here, however generally most plugins
    ;; have resources (e.g. a connection) to clean up
    )

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    ;; When a message is fully acked you can remove it from pending-messages
    ;; Generally this can be left as is.
    (swap! pending-messages dissoc segment-id))

  (retry-segment
    [_ {:keys [rabbitmq/example-datasource] :as event} segment-id]
    ;; Messages are retried if they are not acked in time
    ;; or if a message is forcibly retried by flow conditions.
    ;; Generally this takes place in two steps
    ;; Take the message out of your pending-messages atom, and put it
    ;; back into a datasource or a buffer that are you are reading into
    (when-let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id)
      (swap! example-datasource conj msg)))

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
        drained? (atom false)]
    (->ExampleInput max-pending batch-size batch-timeout pending-messages drained?)))
