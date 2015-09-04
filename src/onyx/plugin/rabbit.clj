(ns onyx.plugin.rabbit
  (:require [less.awful.ssl :as awful]
            [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc])
  (:import (java.io FileNotFoundException)
           (java.security.cert CertificateException)))


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
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                   (String. payload "UTF-8") delivery-tag content-type type)))

(defn start-consumer
  [params]
  (let [conn   (rmq/connect (params->config params))
        ch     (lch/open conn)
        qname  (:queue-name params)
        _      (lq/declare ch qname {:exclusive false :auto-delete true})]
    (prn "Subscribing to " qname)
    (lc/subscribe ch qname message-handler {:auto-ack true})
    {:conn conn :ch ch}))


(defn stop
  [{:keys [conn ch]}]
  (prn "Closing rabbit connection")
  (rmq/close ch)
  (rmq/close conn))
