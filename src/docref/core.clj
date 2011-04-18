(ns docref.core
  (:require [couchdb.client :as couchdb]
	    [somnium.congomongo :as mongodb]))

; Document protocol

(defprotocol Document
  (doc-update! [this f])
  (doc-compare-and-set! [this expected-val new-val]))


; CouchDB implementation

(defn- normalize-url
  "If not present, appends a / to the url-string."
  [url]
  (if-not (= (last url) \/)
    (str url \/ )
    url))

(comment {:results [{:seq 22,
		     :id "cf1b18a6adac0e30085354ab1a00af26",
		     :changes [{:rev "2-0cfd832a52fbc420b220e1c0c6dda5fa"}]}],
	  :last_seq 22})

; Kunne med fordel bruge mapped-reference med keywords

(def poll-done (atom {}))
(def poll-since (atom {}))

(defn terminate-longpoll [server database]
  (swap! poll-done assoc [server database] true))

(def couchdb-listeners (atom {}))

(defn longpoll-fn [server database]
  (swap! poll-done assoc [server database] false)
  (swap! poll-since assoc [server database] (inc (:update_seq (couchdb/database-info server database))))
  (fn []
    (while (not (@poll-done [server database]))
      (let [since (@poll-since [server database])
	    response (:json (couchdb/couch-request
			     {:url (str (normalize-url server) database "/_changes?feed=longpoll&since=" since),
			      :method :get}))
	    since (:last_seq response)]
	(doseq [id (map :id (:results response))]
	  (let [fs (get @couchdb-listeners [server database id] [])]
	    (doseq [f fs] (f))))
	(swap! poll-since assoc [server database] since)))))

(defn start-longpoll-thread [server database]
  (.start (Thread. (longpoll-fn server database))))

; key is ignored for now
(defn add-couchdb-listener
  "Adds a listener for changes to the document with the given id
   on the couchdb database specified by the server and database."
  [server database id key f]
  (swap!
   couchdb-listeners
   (fn [listeners]
     (let [map-key [server database id]
	   listeners (if (contains? listeners map-key) listeners (assoc listeners map-key []))
	   new-val (conj (listeners map-key) f)
	   listeners (assoc listeners map-key new-val)]
       (when (not (contains? @poll-done [server database]))
	 (println "Starting thread")
	 (start-longpoll-thread server database))
       (println listeners)
       listeners))))

(defn remove-couchdb-listener
  [server database id key]
  (swap!
   couchdb-listeners
   (fn [listeners]
     (let [map-key [server database id]]
       (dissoc listeners map-key)))))

;;;;;;;;;;;;


(defn- couchdb-handle-db-result [m]
  (with-meta (dissoc m :_id :_rev) {:rev (:_rev m)}))

(defrecord CouchDBDocument [host database id]
  clojure.lang.IRef
  (deref [this] (couchdb-handle-db-result (couchdb/document-get host database id)))
  (addWatch [this key callback]
	    (do
	      (add-couchdb-listener
	       host database id key
	       (fn [] (callback key :ref :old-state (deref this))))
	      this))
  (removeWatch [this key] (remove-couchdb-listener host database id key))

  Document
  (doc-update!
   [this f]
   (let [cur @this
	 new-val (assoc (f cur) :_rev (:rev (meta cur)))]
     (try
       (couchdb-handle-db-result
	(couchdb/document-update host database id new-val))
       (catch Exception e (doc-update! this f)))))
  (doc-compare-and-set!
   [this expected-val new-val]
   (let [cur @this
	 new-val (assoc new-val :_rev (:rev (meta cur)))]
     (if (= cur expected-val)
       (try
	 (do
	   (couchdb/document-update host database id new-val)
	   true)
	 (catch Exception e (doc-compare-and-set! this expected-val new-val)))
       false))))

(defn couchdb-doc [host database m]
  (let [inserted-record (couchdb/document-create host database m)]
    (CouchDBDocument. host database (:_id inserted-record))))


; MongoDB implementation

(defn- mongodb-handle-db-result [m]
  (dissoc m :_id))

(defrecord MongoDBDocument [collection id]
  clojure.lang.IRef
  (deref [this] (mongodb-handle-db-result (mongodb/fetch-by-id collection id)))
  
  Document
  (doc-update!
   [this f]
   (let [cur @this
	 new-val (f cur)]
     (mongodb/update! collection (assoc cur :_id id) new-val)
     new-val))
  (doc-compare-and-set!
   [this expected-val new-val]
   (let [cur @this]
     (if (= cur expected-val)
       (do
	 (mongodb/update! collection (assoc cur :_id id) new-val)
	 true)
       false))))

(defn mongodb-doc [collection m]
  (let [inserted-record (mongodb/insert! collection m)]
    (MongoDBDocument. collection (:_id inserted-record))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Future ideas 

; TODO: Kan man spec'e at de skal vaere derefs?

; Watches

(comment
  
(defn- normalize-url
  "If not present, appends a / to the url-string."
  [url]
  (if-not (= (last url) \/)
    (str url \/ )
    url))

(defn changes 
  [server database]
  (let [since (inc (:update_seq (couchdb/database-info server database)))]
    (println "since:" since)
    (println
     (:json (couchdb/couch-request
	     {:url (str (normalize-url server) database "/_changes?feed=longpoll&since=" since),
	      :method :get})))))

(comment {:results [{:seq 22,
		     :id "cf1b18a6adac0e30085354ab1a00af26",
		     :changes [{:rev "2-0cfd832a52fbc420b220e1c0c6dda5fa"}]}],
	  :last_seq 22})

(def example-host "http://localhost:5984/")
(def example-database "example-database")

(println (changes example-host example-database))

)

; Riak implementation