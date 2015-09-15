(defproject onyx-rabbitmq "0.7.0-SNAPSHOT"
  :description "Onyx plugin for rabbitmq"
  :url "FIX ME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.onyxplatform/onyx "0.7.3"]
                 [com.novemberain/langohr "3.3.0"]
                 [less-awful-ssl "1.0.0"]
                 [environ "1.0.0"]]
  :plugins [[lein-environ "1.0.0"]]
  :profiles {:dev {:dependencies []
                   :plugins []}})
