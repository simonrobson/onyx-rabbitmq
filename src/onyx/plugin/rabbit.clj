(ns onyx.plugin.rabbit
  (:require [less.awful.ssl :as awful]
            [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [clojure.core.async :refer [>!! chan go-loop <!]]
            [clojure.edn :as edn]
            [taoensso.timbre :refer [debug info] :as timbre])
  (:import (java.io FileNotFoundException)
           (java.security.cert CertificateException)))

(defn edn-serializer
  [data]
  (.getBytes (prn-str data)))

(defn edn-deserializer
  [bytes]
  (edn/read-string (String. bytes)))

(defn create-ssl-context
  "Returns an SSL context for use when initiating an SSL connection.
   Parameters are absolute paths to SSL credentials."

  [client-key client-crt ca-crt]
  (try
    (awful/ssl-context client-key client-crt ca-crt)

    (catch FileNotFoundException e
      (let [file (clojure.string/replace (.getMessage e) #" \(No such file or directory\)" "")]
        (throw (ex-info "Failed to create an SSL context (file not found)" {:file file}))))
    (catch NullPointerException e
      (throw (ex-info "Failed to create an SSL context (null pointer. maybe an invalid ssl key)"
                      {:key client-key})))
    (catch CertificateException e
      (throw (ex-info "Failed to create an SSL context (invalid ssl certificate)"
                      {:crt client-crt :ca-crt ca-crt})))))

(defn params->config
  [params]
  (dissoc (if (not-any? nil? (map params [:key :crt :ca-crt]))
            (assoc params
                   :ssl true
                   :authentication-mechanism "EXTERNAL"
                   :ssl-context (apply create-ssl-context (map params [:key :crt :ca-crt])))
            (dissoc params :key :crt :ca-crt))
          :queue-name :key :crt :ca-crt))

(defn message-handler
  [write-to-chan deserialize-fn]
  (fn
    [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
    (>!! write-to-chan {:payload (deserialize-fn payload) :delivery-tag delivery-tag})))

(defn start-consumer
  [params deserialize-fn write-to-ch]
  (let [conn   (rmq/connect (params->config params))
        ch     (lch/open conn)
        qname  (:queue-name params)]
    (info "Starting consumer on queue"  qname)
    (lq/declare ch qname {:exclusive false :durable true :auto-delete false})
    (lc/subscribe ch qname (message-handler write-to-ch deserialize-fn) {:auto-ack false})
    {:conn conn :ch ch}))

(defn start-publisher
  [params serialize-fn]
  (let [write-ch (chan 2)
        conn   (rmq/connect (params->config params))
        ch     (lch/open conn)
        qname  (:queue-name params)]
    (lq/declare ch qname {:exclusive false :durable true :auto-delete false})
    (go-loop []
      (let [msg (<! write-ch)]
        (lb/publish ch "" qname (serialize-fn msg))
        (recur)))
    {:conn conn :ch ch :write-ch write-ch}))

(defn ack
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn requeue
  [ch delivery-tag]
  (lb/reject ch delivery-tag true))

(defn stop
  [{:keys [conn ch]}]
  (info "Closing rabbit connection")
  (rmq/close ch)
  (rmq/close conn))
