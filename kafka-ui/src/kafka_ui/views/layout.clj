(ns kafka-ui.views.layout
  (:require [hiccup.page :refer [html5 include-css include-js]]))

(defn common [& body]
  (html5
    [:head
     [:title "Welcome to kafka-ui"]
     (include-css "/css/screen" "/css/bootstrap-theme.min.css")
     (include-js "/js/bootstrap.min.js")
     (include-js "/js/jquery-1.11.0.min.js")]
    [:body body]))


