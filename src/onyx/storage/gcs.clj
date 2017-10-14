(ns onyx.storage.gcs
  (:require [onyx.checkpoint :as checkpoint]
            [onyx.monitoring.metrics-monitoring :as m]
            [onyx.static.default-vals :refer [arg-or-default]]
            [taoensso.timbre :refer [info error warn trace fatal debug] :as timbre]
            [clojure.java.io :as io])
  (:import [com.google.api.client.googleapis.auth.oauth2 GoogleCredential]
           [com.google.cloud.storage
            Storage StorageOptions
            BucketInfo Bucket
            BlobId Blob BlobInfo]
           [com.google.auth.oauth2 ServiceAccountCredentials]
           [java.io FileInputStream]))

(declare scopes-empty?)

(defn new-client ^Storage [peer-config]
  (case (arg-or-default :onyx.peer/storage.gcs.auth-type peer-config)
    :path (let [path (or (:onyx.peer/storage.gcs.auth.path.credentials-path peer-config)
                         (throw (Exception. ":onyx.peer/storage.gcs.auth.path.credentials-path must be set when using :onyx.peer/storage.gcs.auth-type of :path")))
                _ (if-not (.exists (io/file path))
                    (throw (Exception. ":onyx.peer/storage.gcs.auth.path.credentials-path is set but the path could not be found. If this is running in a container; has the path been mountted as a volume?")))
                credentials (ServiceAccountCredentials/fromStream
                             (FileInputStream. path))
                storage (-> (StorageOptions/newBuilder)
                            (.setCredentials credentials)
                            (.build)
                            (.getService))]
            storage)
    :config (let [client-id (or (:onyx.peer/storage.gcs.auth.config.client-id peer-config)
                                (throw (Exception. ":onyx.peer/storage.gcs.auth.config.client-id must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  client-email (or (:onyx.peer/storage.gcs.auth.config.client-email peer-config)
                                   (throw (Exception. ":onyx.peer/storage.gcs.auth.config.client-email must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  private-key (or (:onyx.peer/storage.gcs.auth.config.private-key peer-config)
                                  (throw (Exception. ":onyx.peer/storage.gcs.auth.config.private-key must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  private-key-id (or (:onyx.peer/storage.gcs.auth.config.private-key-id peer-config)
                                     (throw (Exception. ":onyx.peer/storage.gcs.auth.config.private-key-id must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  _ (if (scopes-empty? peer-config)
                      (warn ":onyx.pexber/storage.gcs.auth.config.scopes not supplied. Defaulting to [\"https://www.googleapis.com/auth/devstorage.read_write\"]"))
                  scopes (arg-or-default :onyx.peer/storage.gcs.auth.config.scopes peer-config)
                  credentials (ServiceAccountCredentials/fromPkcs8
                               client-id client-email
                               private-key private-key-id
                               scopes)
                  storage (-> (StorageOptions/newBuilder)
                              (.setCredentials credentials)
                              (.build)
                              (.getService))]
              storage)))

(defn- scopes-empty? [peer-config]
  (empty? (:onyx.peer/storage.gcs.auth.config.scopes peer-config)))

#_(defmulti onyx.checkpoint/storage :gcs [peer-config monitoring])
