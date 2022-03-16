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
                 :_predicate/fullText false}
                {:_id               :_predicate
                 :_predicate/name   :event/order
                 ;; NOTE: the "strictly increasing" condition could be validated via specs:
                 ;; - collection spec to ensure order is there
                 ;; - predicate spec to ensure the new number is bigger than the max
                 ;; This validation isn't happening here, we're just transacting "correct" data.
                 :_predicate/doc    "Strictly increasing big int for event ordering."
                 :_predicate/unique true
                 :_predicate/type   :bigint}])


(defn new-event [x]
  {:_id        :event
   :event/id   (str "uuid-" x)
   :event/data "{}"
   ;; Compute the new order number as 1 higher than last.
   ;; NOTE: is max-pred-val efficient for very large collections? I don't know.
   :event/order "#(inc (max-pred-val \"event/order\"))"})

(defn spy
  ([x]
   (println x)
   x)
  ([prefix x]
   (println prefix x)
   x))

(comment
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
        ;; Change these number to make more events, or to create sub ranges.
        (range 0 1000))

  (println "Query over all events")
  (time (-> conn
            (fdb/db ledger)
            (fdb/query {:select {"?event" ["*"]}
                        :where  [["?event" "event/id", "?order"]]
                        ;; Subject (?event here) is a monotonically incrementing bigint,
                        ;; so ordering by that gives us insertion order.
                        :opts   {:orderBy ["ASC", "event/order"]
                                 :limit 15000}})
            deref
            count))
  ;; Takes ~100ms for 1000 total events
  ;; Takes ~500ms for 5000 total events

  (println "Query over half the events")

  ;; Find out the order number of a target id.
  ;; NOTE: I tried doing this within the filtered query but wasn't able to unify
  ;; ?order within the object-function.
  (def target-id "uuid-500")
  (def target-order (-> conn
                        (fdb/db ledger)
                        (fdb/query {:select "?order"
                                    :where  [["?event" "event/id", target-id]
                                             ["?event" "event/order" "?order"]]})
                        deref
                        first))

  (time (-> conn
            (fdb/db ledger)
            (fdb/query {:select {"?event" ["*"]}
                        :where  [["?event" "event/order" (str "#(> ?o " target-order ")")]]
                        :opts   {:orderBy ["ASC", "event/order"]
                                 :limit 15000}})
            deref
            count))
  ;; Takes ~300ms for 1000 total events (500 returned)
  ;; Takes ~300ms for 5000 total events (2500 returned)
  ;; Takes ~150ms for 5000 total events (1500 returned)
  ;; Takes ~50ms for 5000 total events (500 returned)
  ;; Takes ~2ms for 5000 total events (5 returned)


  (println "Delete ledger")
  ;; Delete ledger and close conn.
  @(fdb/delete-ledger conn ledger)
  (fdb/close conn)

  )
