(ns kafka-ui.routes.home
  (:require [compojure.core :refer :all]
            [kafka-ui.views.layout :as layout]
            [kafka-ui.models.db :as db]
            [kafka-ui.models.kafka :as kafka]
            [hiccup.element :refer :all]
            [hiccup.form :refer :all]))



(defn get-group-tooltip [group]
  (kafka/get-consumer-members group))

(defn get-topic-tooltip [offsets topic]
  (kafka/get-brokers offsets topic))

(defn get-saved-offset-lag-sum [offsets group topic offset-total]
  "Gets the sum of the offset lag for all partitions of a topic"
  (apply + (map #(- offset-total %)
          (vals (kafka/get-saved-topic-offsets offsets group topic)))))


(defn home [& [groups]]
  (let [offsets (kafka/get-offsets)]
    (layout/common
     [:div
      [:h1 "Kafka Offset View"]]
     [:div {:class "panel panel-default"}
      [:div {:class "panel-heading"} "Kafka Topics"]
       [:div
       [:table#offset_table.table
         [:thead
          [:tr
           [:th "Topic1"]
           [:th "Offsets Total"]
           (for [group groups] [:th group])
           ]]
           [:tbody
             (for [topic (kafka/get-topics offsets)
                   :let [offset-total (kafka/offset-total offsets topic)]]
               [:tr
                [:td topic]
                [:td offset-total]
                (for [group groups]
                  [:td [:a {:id (str "group-" group) :href (str "/locks/" group "/" topic)}
                        (get-saved-offset-lag-sum offsets group topic offset-total)]])
                ]
               )]]]
        ]

        (javascript-tag
         (apply
          str

          (for [group groups]
            (str
             "$(document).ready(function(){
             $('#group-" group "').tooltip({
             title : '" (get-group-tooltip group) "'
             });"
            ))))
      )))




(defroutes home-routes
  (GET "/" [] (home (kafka/get-groups))))
