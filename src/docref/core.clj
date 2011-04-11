(ns docref.core
  (:require [couchdb.client :as couchdb]
	    [somnium.congomongo :as mongodb]))

; Document protocol

(defprotocol Document
  (doc-update! [this f])
  (doc-compare-and-set! [this expected-val new-val]))


; CouchDB implementation

(defn- couchdb-handle-db-result [m]
  (with-meta (dissoc m :_id :_rev) {:rev (:_rev m)}))

(defrecord CouchDBDocument [host database id]
  clojure.lang.IRef
  (deref [this] (couchdb-handle-db-result (couchdb/document-get host database id)))

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
  (let [since (inc (:update_seq (database-info example-host example-database)))]
    (:json (couch-request {:url (str (normalize-url server) database "/_changes?feed=longpoll&since=" since),
			   :method :get}))))

(comment println (changes example-host example-database))
)

; Riak implementation