(ns kda-sim.ddl
  (:require [camel-snake-kebab.core :as csk]
            [taoensso.nippy :as nippy]
            [next.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            [cheshire.core :as json]))





(defn drop-table [conn]
    (next.jdbc/execute! conn ["drop table event"]))

(defn select-event [conn limit]
    (next.jdbc/execute! conn [(str "select * from event limit " limit)]))


(defn delete-table [conn]
  (next.jdbc/execute! conn ["delete event"]))

(defn add-index[conn]
  (jdbc/execute! conn ["create index evendate_index on event (eventdate asc) "]))