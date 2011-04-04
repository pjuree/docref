(ns docref.core
  (:use couchdb.client))

(def example-host "http://localhost:5984/")
(def example-database "foo")

; Only do this once
; (database-create example-host example-database)

(defprotocol Document
  (doc-update! [this f])
  (doc-compare-and-set! [this expected-val new-val]))

(defn handle-rev-meta [m]
  (with-meta (dissoc m :_id :_rev) {:rev (:_rev m)}))

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

(println (changes example-host example-database))

(defrecord CouchDBDocument [host database id]
  clojure.lang.IRef
  (deref [this] (handle-rev-meta (document-get host database id)))
  Document
  (doc-update!
   [this f]
   (let [cur @this
	 new-val (assoc (f cur) :_rev (:rev (meta cur)))]
     (try
       (handle-rev-meta
	(document-update host database id new-val))
       (catch Exception e (doc-update! this f)))))
  (doc-compare-and-set!
   [this expected-val new-val]
   (let [cur @this
	 new-val (assoc new-val :_rev (:rev (meta cur)))]
     (if (= cur expected-val)
       (try
	 (do
	   (document-update host database id new-val)
	   true)
	 (catch Exception e (doc-compare-and-set! this expected-val new-val)))
       false))))

(defn make-document [host database m]
  (let [inserted-record (document-create host database m)]
    (CouchDBDocument. host database (:_id inserted-record))))

(let [foo (make-document example-host example-database {:bar "This is a document"})]
  (println @foo)
  (println (meta @foo))
  (println (doc-update! foo (fn [m] (assoc m :bar "This is the altered document"))))
  (println)
  (println (doc-compare-and-set! foo @foo {:foobar "Dada"}))
  (println (doc-compare-and-set! foo {} {:foobar "Dada"}))
  (println (doc-compare-and-set! foo {:foobar "Dada"} {:foo :baz}))
  (println @foo))