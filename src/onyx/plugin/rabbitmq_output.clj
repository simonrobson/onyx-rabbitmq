(ns onyx.plugin.rabbitmq-output
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [taoensso.timbre :refer [debug info] :as timbre]))

(defn inject-writer
  [event lifecycle]
  (when-not (:rabbitmq/example-datasink event)
    (throw (ex-info ":example-output/example-datasink not found - add it using a :before-task-start lifecycle"
                    {:event-map-keys (keys event)})))
  {})

;; map of lifecycle calls that are required to use this plugin
;; users will generally always have to include these in their lifecycle calls
;; when submitting the job
(def writer-calls
  {:lifecycle/before-task-start inject-writer})

(defrecord ExampleOutput []
  ;; Read batch can generally be left as is. It simply takes care of
  ;; receiving segments from the ingress task
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    ;; Write the batch that was read out to your datasink. 
    ;; In this case we are swapping onto a collection in an atom
    ;; Messages are on the leaves :tree, as :onyx/fn is called
    ;; and each incoming segment may return n segments
    [_ {:keys [onyx.core/results rabbitmq/example-datasink] :as event}]
    (doseq [msg (mapcat :leaves (:tree results))]
      (swap! example-datasink conj (:message msg)))
    {})

  (seal-resource 
    ;; Clean up any resources you opened.
    ;; If relevant, put a :done on your datasource so that
    ;; any readers will know the data sink has been sealed
    [_ {:keys [rabbitmq/example-datasink]}]
    (swap! example-datasink conj :done)))


;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn output [pipeline-data]
  (->ExampleOutput))
