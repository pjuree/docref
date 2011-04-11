(ns docref.example
  (:require [docref.core :as docref]
	    [couchdb.client :as couchdb]
	    [somnium.congomongo :as mongodb]))

(def initial-map {:bar "This is a document"})

(defn example [text doc]
  (println "------------------------------")
  (println text)
  
  (println "@:" @doc)
  (println "meta:" (meta @doc))

  (println "doc-update:" (docref/doc-update! doc (fn [m] (assoc m :bar "This is the altered document"))))
  (println "afterwards:" @doc)
  
  (println "compare-and-set (on doc):" (docref/doc-compare-and-set! doc @doc {:foobar "Dada"}))
  (println "compare and set (on garbage):" (docref/doc-compare-and-set! doc {} {:foobar "Dada"}))
  (println "compare and set (on identical):" (docref/doc-compare-and-set! doc {:foobar "Dada"} {:foo :baz}))
  (println "@:" @doc))

; CouchDB
(def example-couchdb-host "http://localhost:5984/")
(def example-couchdb-database "example-database")
(when-not (contains? (set (couchdb/database-list example-couchdb-host)) example-couchdb-database)
  (couchdb/database-create example-couchdb-host example-couchdb-database))

(example
 "CouchDB"
 (docref/couchdb-doc example-couchdb-host example-couchdb-database initial-map))

; MongoDB

(def example-mongodb-database "example-database")
(def example-mongodb-collection :example-collection)
(mongodb/mongo! :db example-mongodb-database)

(example
 "MongoDB"
 (docref/mongodb-doc example-mongodb-collection initial-map))