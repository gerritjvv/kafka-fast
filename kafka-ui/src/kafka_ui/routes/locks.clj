(ns kafka-ui.routes.locks
  (:require [compojure.core :refer :all]
            [kafka-ui.views.layout :as layout]
            [kafka-ui.models.kafka :as kafka]))



(defn show-topic-locks [group topic]
  (layout/common
   [:div
    [:h1 "Kafka Lock View"]]
   [:div {:class "panel panel-default"}
    [:div {:class "panel-heading"} "Kafka Lock View"]
     [:div
     [:table#lock_table.table
       [:thead
        [:tr
         [:th "Path"]
         [:th "Consumer"]
         ]]
         [:tbody

           (for [[path {:keys [member]}] (kafka/get-locks (kafka/get-offsets) group topic)]
             [:tr [:td path] [:td member]])]]]]))

(defroutes lock-routes
  (GET "/locks/:group/:topic" [group topic] (show-topic-locks group topic)))
