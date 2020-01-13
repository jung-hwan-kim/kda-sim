(ns kda-sim.data-analysis
  (:require [datascript.core :as d]
            [camel-snake-kebab.core :as csk]
            [taoensso.nippy :as nippy]
            [next.jdbc :as jdbc]
            [kda-sim.aws :as aws]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            [cheshire.core :as json]))


(defn create-event-table [conn]
  (jdbc/execute! conn ["create table event (
  id int auto_increment primary key,
  vehicleId int,
  eventDate varchar(32),
  eventType varchar(32),
  country int,
  runNumber varchar(10),
  lotNumber varchar(10),
  lane varchar(10),
  iteration int,
  currentHighBid int,
  buyNowPrice int,
  auctionEndDate varchar(32),
  vehicleGrade decimal(3,2)
)"]))

(defn -loadup[conn file]
  (println "loading.." (str file))
  (let [jsons (json/parsed-seq (clojure.java.io/reader file) true)
        val (map (fn[x] (select-keys x [:vehicleId
                                      :eventType
                                      :eventDate
                                      :eventType
                                      :country
                                      :runNumber
                                      :lotNumber
                                      :lane
                                      :iteration
                                      :currentHighBid
                                      :buyNowPrice
                                      :auctionEndDate
                                      :vehicleGrade])) jsons)
        i-sql (-> (insert-into :event)
                  (values val)
                  sql/format)
        result (jdbc/execute! conn i-sql)
      ]
    (println "finished: " result)))

(defn loadup[conn]
  (let [dir (clojure.java.io/file "../../tmp/drivin-prod")
        ]
    (loop [fs (file-seq dir)]
      (if (empty? fs)
        "done"
        (let [f (first fs)]
          (if (.isFile f)
            (-loadup conn f))
          (recur (rest fs)))))))

(defn execute[conn query]
  (jdbc/execute! conn [query]))

(defn query[conn vehicleid]
  (jdbc/execute! conn [(str "select VEHICLEID, eventtype, eventdate from event where vehicleId = " vehicleid " order by eventdate")] {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps}))

(defn query-next[conn size]
  (jdbc/execute! conn [(str "select VEHICLEID, eventdate  from event order by eventdate limit " size)] {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps}))


(defn query-c[vehicleid]
  (map (fn[x] (map val x)) (execute "select eventdate, eventtype, vehiclegrade, iteration, lotnumber, runnumber, lane, auctionenddate, buynowprice, currenthighbid from event where vehicleId = 531659772 order by eventdate"))
  )

(defn maintanance[conn]
  (jdbc/execute! conn ["create index evendate_index on event (eventdate asc) "]))

;(da/execute "select count(distinct vehicleid) from event")
; => [{:COUNT(DISTINCT VEHICLEID) 167694}]

(defn get-next-events
  ([conn offset-eventdate size]
   (jdbc/execute! conn [(str "select * from event where eventdate > '" offset-eventdate "' order by eventdate limit " size)] {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps}))
  ([conn vehicleid offset-eventdate size]
   (jdbc/execute! conn [(str "select * from event where vehicleid = " vehicleid " and eventdate > '" offset-eventdate "' order by eventdate limit " size)] {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps}))
  )



(defn apply-batch-events
  ([conn fn batch-size]
    (loop [r (get-next-events conn "2020-01-01" batch-size) c 0]
      (if (empty? r)
        "done"
        (let [offset-eventdate (:eventdate (last r))]
          (println c (:eventdate (first r)) offset-eventdate)
          (fn r)
          (recur (get-next-events conn offset-eventdate batch-size) (inc c))))))
  ([conn vehicleid fn batch-size]
   (loop [r (get-next-events conn vehicleid "2020-01-01" batch-size) c 0]
     (if (empty? r)
       "done"
       (let [offset-eventdate (:eventdate (last r))]
         (println c (:eventdate (first r)) offset-eventdate)
         (fn r)
         (recur (get-next-events conn vehicleid offset-eventdate batch-size) (inc c)))))))


(defn batchload-event-data
  ([stream-name batch-size]
   (let[db {:dbtype "h2" :dbname "inventory"}
        conn (jdbc/get-connection db)
        fn (fn[r] (aws/kinesis-put stream-name r))]
     (apply-batch-events conn fn batch-size)))
  ([stream-name vehicleid batch-size]
   (let[db {:dbtype "h2" :dbname "inventory"}
        conn (jdbc/get-connection db)
        fn (fn[r] (aws/kinesis-put stream-name r))]
     (apply-batch-events conn vehicleid fn batch-size))))
