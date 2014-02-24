(ns kafka-ui.models.conf
  (:require [clojure.edn :as edn]))

(defn load-config []
  "Loads the configuration in an edn format once from the /etc/kafka-ui.clj file"
  (edn/read-string (slurp "/etc/kafka-ui/conf.clj")))

