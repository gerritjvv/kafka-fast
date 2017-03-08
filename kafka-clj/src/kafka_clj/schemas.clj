(ns kafka-clj.schemas
  (:require [schema.core :as s]))


(def WORK-UNIT-SCHEMA {:len                       s/Int,
                       :max-offset                s/Int,
                       :offset                    s/Int,
                       :partition                 s/Int,
                       (s/optional-key :producer) (s/either (s/pred nil?)
                                                            {:host       s/Str, :port s/Int, :isr [{:host s/Str :port s/Int}],
                                                             :id         s/Int,
                                                             :error-code s/Int}),
                       (s/optional-key :status)   s/Keyword
                       :topic                     s/Str,
                       :ts                        s/Int


                       s/Any                      s/Any})

(def PARTITION-OFFSET-DATA {:offset s/Int :all-offsets [s/Int] :error-code s/Int :partition s/Int s/Any s/Any})

(def TOPIC-SCHEMA s/Str)

(def PARTITION-OFFSET-SCHEMA {:partition s/Int :offset s/Int})

(def PARTITIONS-OFFSET-SCHEMA [PARTITION-OFFSET-SCHEMA])

;;define single partition segment maps
(def PARITION-SEGMENT {:partition s/Int})