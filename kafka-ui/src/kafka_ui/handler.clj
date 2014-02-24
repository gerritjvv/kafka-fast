(ns kafka-ui.handler
  (:require [compojure.core :refer [defroutes routes]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.file-info :refer [wrap-file-info]]
            [hiccup.middleware :refer [wrap-base-url]]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [kafka-ui.routes.home :refer [home-routes]]
            [kafka-ui.routes.locks :refer [lock-routes]]))

(defn init []
  (println "kafka-ui is starting"))

(defn destroy []
  (println "kafka-ui is shutting down"))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(def app
  (-> (routes home-routes lock-routes app-routes)
      (handler/site)
      (wrap-base-url)))


