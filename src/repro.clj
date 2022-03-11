(ns repro
  (:require [fluree.db.api :as fdb]))


(def ledger "events/log")
(def schema-tx [{:_id              :_collection
                 :_collection/name :event
                 :_collection/doc  "Athens semantic events."}
                {:_id               :_predicate
                 :_predicate/name   :event/id
                 :_predicate/doc    "A globally unique event id."
                 :_predicate/unique true
                 :_predicate/type   :string
                 :_predicate/fullText false}
                {:_id             :_predicate
                 :_predicate/name :event/data
                 :_predicate/doc  "Event data serialized as an EDN string."
                 :_predicate/type :string
                 :_predicate/fullText false}])


(defn new-event [x]
  {:_id        :event
   :event/id   (str "uuid-" x)
   :event/data "{}"})

(defn spy
  ([x]
   (println x)
   x)
  ([prefix x]
   (println prefix x)
   x))

(comment
  ;; Create ledger
  (println "Create ledger")
  (def conn (fdb/connect "http://localhost:8090"))
  @(fdb/new-ledger conn ledger)
  (fdb/wait-for-ledger-ready conn ledger)
  @(fdb/transact conn ledger schema-tx)

  (println "Create 1k events")
  (run! #(->> %
              (spy "event #")
              new-event
              vector
              (fdb/transact conn ledger)
              deref)
        ;; Change this number to make more events
        (take 1000 (range)))


  (println "Query over all events")
  (time (-> conn
            (fdb/db ledger)
            (fdb/query {:select {"?event" ["*"]}
                        :where  [["?event" "event/id", "?id"]]
                        ;; Subject (?event here) is a monotonically incrementing bigint,
                        ;; so ordering by that gives us insertion order.
                        :opts   {:orderBy ["ASC", "?event"]
                                 :limit 15000}})
            deref
            count))
  ;; Takes ~100ms for 1000 total events
  ;; Takes ~500ms for 5000 total events

  (println "Query over half the events")
  (let [event-number-500-subject-id (-> conn
                                        (fdb/db ledger)
                                        (fdb/query {:select "?event"
                                                    :where  [["?event" "event/id", "uuid-500"]]})
                                        deref
                                        first)]
    (time (-> conn
            (fdb/db ledger)
            (fdb/query {:select {"?event" ["*"]}
                        :where  [["?event" "event/id", "?id"]
                                 ;; Add a filter to only get half of the total events
                                 {:filter [(str "(> ?event " event-number-500-subject-id ")")]}]
                        :opts   {:orderBy ["ASC", "?event"]
                                 :limit 15000}})
            deref
            count)))
  ;; Takes ~300ms for 1000 total events (500 returned)
  ;; Takes ~2000ms for 5000 total events (2500 returned)


  (println "Delete ledger")
  ;; Delete ledger and close conn.
  @(fdb/delete-ledger conn ledger)
  (fdb/close conn)

  )
