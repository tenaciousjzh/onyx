(ns onyx.storage.gcs
  (:require [onyx.checkpoint :as checkpoint]
            [onyx.monitoring.metrics-monitoring :as m]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [ms->ns ns->ms]]
            [taoensso.timbre :refer [info  error  warn  trace  fatal  debug
                                     infof errorf warnf tracef fatalf debugf] :as timbre]
            [clojure.java.io :as io]
            [clojure.string :as st])
  (:import [com.google.api.client.googleapis.auth.oauth2 GoogleCredential]
           [com.google.cloud.storage
            Storage StorageOptions StorageClass
            BucketInfo Bucket
            BlobId Blob BlobInfo
            Storage$BucketGetOption
            Storage$BucketField
            Storage$BlobGetOption
            Storage$BlobField
            StorageException
            StorageOptions$Builder
            Storage$BucketTargetOption
            Bucket$BucketSourceOption]
           [com.google.auth.oauth2 ServiceAccountCredentials]
           [java.io FileInputStream InputStream]
           [java.nio ByteBuffer]
           [java.util.concurrent.atomic AtomicLong]
           [java.util.concurrent.locks LockSupport]))

(declare scopes-empty?)

(defn new-client ^Storage [peer-config]
  "Creates a com.google.cloud.storage.Storage API client for Google Cloud Storage to manage buckets and blobs.
  Args: Map peer config containing the configuration needed to create a Storage Client. See onyx.information-model
        for detailed descriptions on the configuration keys needed.
  Returns: a com.google.cloud.storage.Storage service client."
  (case (arg-or-default :onyx.peer/storage.gcs.auth-type peer-config)
    :path (let [_ (infof "Authenticating with JSON Key file located at: %s"
                         (:onyx.peer/storage.gcs.auth.path.credentials-path peer-config))
                path (or (:onyx.peer/storage.gcs.auth.path.credentials-path peer-config)
                         (throw (Exception. ":onyx.peer/storage.gcs.auth.path.credentials-path must be set when using :onyx.peer/storage.gcs.auth-type of :path")))
                _ (if-not (.exists (io/file path))
                    (throw (Exception. ":onyx.peer/storage.gcs.auth.path.credentials-path is set but the path could not be found. If this is running in a container; has the path been mounted as a volume?")))
                project-id (:onyx.peer/storage.gcs.project-id peer-config)
                _ (if (st/blank? project-id)
                    (throw (Exception. ":onyx.peer/storage.gcs.project-id must be set to continue.")))
                credentials (ServiceAccountCredentials/fromStream
                             ^InputStream (FileInputStream. path))
                builder ^StorageOptions$Builder (StorageOptions/newBuilder)
                storage (-> builder
                            (.setCredentials credentials)
                            (.setProjectId project-id)
                            (.build)
                            (.getService))]
            storage)
    :config (let [client-id (or (:onyx.peer/storage.gcs.auth.config.client-id peer-config)
                                (throw (Exception. ":onyx.peer/storage.gcs.auth.config.client-id must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  _ (infof "Client ID: %s" client-id)
                  client-email (or (:onyx.peer/storage.gcs.auth.config.client-email peer-config)
                                   (throw (Exception. ":onyx.peer/storage.gcs.auth.config.client-email must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  _ (infof "Client Email: %s" client-email)
                  private-key (or (:onyx.peer/storage.gcs.auth.config.private-key peer-config)
                                  (throw (Exception. ":onyx.peer/storage.gcs.auth.config.private-key must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  _ (infof "Private Key: %s" private-key)
                  private-key-id (or (:onyx.peer/storage.gcs.auth.config.private-key-id peer-config)
                                     (throw (Exception. ":onyx.peer/storage.gcs.auth.config.private-key-id must be set when using :onyx.peer/storage.gcs.auth-type of :config")))
                  _ (infof "Private Key ID: %s" private-key-id)
                  _ (if (scopes-empty? peer-config)
                      (warn ":onyx.peer/storage.gcs.auth.config.scopes not supplied. Defaulting to [\"https://www.googleapis.com/auth/devstorage.read_write\"]"))
                  scopes (arg-or-default :onyx.peer/storage.gcs.auth.config.scopes peer-config)
                  _ (infof "Scopes: %s" scopes)
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

(defn- get-bucket [storage bucket-name]
  (.get storage bucket-name (into-array [(Storage$BucketGetOption/fields (into-array [Storage$BucketField/NAME]))])))

(defn- get-blob [bucket-svc blob-name]
  (.get bucket-svc blob-name (into-array [(Storage$BlobGetOption/fields (into-array [Storage$BlobField/NAME]))])))

(defn- checkpoint-task-key [tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  (let [prefix-hash (mod (hash [tenancy-id job-id replica-version epoch task-id slot-id]) 100000)]
    (str prefix-hash "_" tenancy-id "/"
         job-id "/"
         replica-version "-" epoch "/"
         (namespace task-id) (if (namespace task-id) "-") (name task-id) "/"
         slot-id "/"
         (name checkpoint-type))))

(defn- str->storage-class [s]
  (cond (= "MULTI_REGIONAL" s) StorageClass/MULTI_REGIONAL
        (= "REGIONAL" s) StorageClass/REGIONAL
        (= "NEARLINE" s) StorageClass/NEARLINE
        (= "COLDLINE" s) StorageClass/COLDLINE
        (= "STANDARD" s) StorageClass/STANDARD
        :else StorageClass/REGIONAL))

;;CheckpointManager stores the service API client to manage buckets and blobs in Google Cloud Storage 
(defrecord CheckpointManager [id ^Storage storage monitoring ^Bucket bucket storage-class location transfers timeout-ns])

(defmethod onyx.checkpoint/storage :gcs [peer-config monitoring]
  "Set up the state needed to reuse the Storage client for interaction with the Google Cloud Storage
   Service.
   Args:
   peer-config - Map containing the key/value pairs needed for managing the bucket that is used for checkpoints. Namely: `:onyx.peer/storage.gcs.bucket`, `:onyx.peer/storage.gcs.storage-class`, and `:onyx.peer/gcs.location`
   monitoring - Handle to the monitoring for reporting metrics on checkpoints.
   Returns: An `onyx.storage.gcs.CheckpointManager`"
  (let [id (java.util.UUID/randomUUID)
        storage (new-client peer-config)
        timeout-ns (ms->ns (arg-or-default :onyx.peer/storage.timeout peer-config))
        bucket-name (or (:onyx.peer/storage.gcs.bucket peer-config)
                        (throw (Exception. ":onyx.peer/storage.gcs.bucket must be set in order to manage checkpoints on Google Cloud Storage.")))
        storage-class (or (:onyx.peer/storage.gcs.storage-class peer-config)
                          (throw (Exception. ":onyx.peer/storage.gcs.storage-class must be set in order to manage checkpoints on Google Cloud Storage.")))
        location (or (:onyx.peer/storage.gcs.location peer-config)
                     (throw (Exception. ":onyx.peer/storage.gcs.location must be set in order to manager checkpoints on Google Cloud Storage")))
        ^Bucket bucket (get-bucket storage bucket-name)
        _ (infof "Bucket from GCS: %s" bucket)
        ;;If service account used in the credentials or JSON Key file does not have proper permissions
        ;;this could throw an exception.
        bucket (if (or (nil? bucket)
                       (not (.exists bucket (make-array com.google.cloud.storage.Bucket$BucketSourceOption 0))))
                 (try (.create storage
                               (-> (BucketInfo/newBuilder bucket-name)
                                   (.setLocation location)
                                   (.setStorageClass (str->storage-class storage-class))
                                   (.build))
                               (make-array com.google.cloud.storage.Storage$BucketTargetOption 0))
                      (catch StorageException ex
                        ;;409 means another thread created the bucket at the same time as the one executing this
                        (if (= 409 (.getCode ex))
                          (do
                            (info "Another thread already created the bucket, retrieving from GCS..")
                            (get-bucket storage bucket-name))
                     nil)))
                 bucket)
        _ (infof "Bucket Info: %s" bucket)]
    (->CheckpointManager id storage monitoring bucket storage-class location (atom []) timeout-ns)))

(defn upload [^Storage storage ^Bucket bucket ^String location ^String storage-class
              ^String key ^bytes serialized ^String content-type]
  (infof "Uploading to bucket: %s, location: %s, storage-class: %s, key: %s, serialized: %s, content-type: %s"
         bucket location storage-class key serialized content-type)
  ;;Note: Google Cloud Storage encrypts all data at rest by default.
  (let [size (alength serialized)
        _ (infof "Bucket from GCS: %s" bucket)
        ;;If service account used in the credentials or JSON Key file does not have proper permissions
        ;;this could throw an exception.
        blob-id (BlobId/of (.getName bucket) key)
        blob-info (-> (BlobInfo/newBuilder blob-id)
                      (.setContentType content-type)
                      (.build))]
    (future
      (try
        (infof "Uploading %s bytes to create checkpoint blob on GCS with blob-info: %s" size blob-info)
        (.create storage blob-info serialized (make-array com.google.cloud.storage.Storage$BlobTargetOption 0))
        (infof "Finished uploading checkpoint: %s" blob-info)
        (catch Exception ex
          (error "Unable to save checkpoint to Google Cloud Storage!" ex)
          (throw (Exception. "Unable to save checkpoint to Google Cloud Storage!" ex)))))))

;;Writes out the checkpoint as a com.google.cloud.storage.Blob in the configured com.google.cloud.storage.Bucket.
;;Args:
;;chk-mgr - An instance of `onyx.storage.gcs.CheckpointManager` record
;;tenancy-id -
;;job-id - The unique id of the job to which this belongs.
;;replica-version -
;;epoch -
;;task-id -
;;slot-id -
;;checkpoint-type -
;;checkpoint-bytes - bytes to be written to cloud storage.
;;Returns: The instance of `onyx.storage.gcs.CheckpointManager` record.
(defmethod onyx.checkpoint/write-checkpoint onyx.storage.gcs.CheckpointManager
  [{:keys [storage bucket storage-class location transfers] :as chk-mgr}
   tenancy-id job-id replica-version epoch
   task-id slot-id checkpoint-type ^bytes checkpoint-bytes]
  (let [k (checkpoint-task-key tenancy-id job-id replica-version epoch task-id
                               slot-id checkpoint-type)
        _ (debugf "Starting checkpoint to gcs under key: %s" k)
        up-future (onyx.storage.gcs/upload storage bucket location storage-class
                                           k checkpoint-bytes "application/octet-stream")]
    (swap! transfers conj {:key k
                           :upload up-future
                           :size-bytes (alength checkpoint-bytes)
                           :start-time (System/nanoTime)})
    chk-mgr))

;;Checks the collection of checkpoint uploads to see if they have completed.
;;Args: `onyx.storage.gcs.CheckpointManager`
;;Returns: true only when all uploads managed by `onyx.storage.gcs.CheckpointManager` have finished.
(defmethod onyx.checkpoint/complete? onyx.storage.gcs.CheckpointManager
  [{:keys [transfers monitoring timeout-ns] :as chk-mgr}]
  (empty?
   ;;The fn performed on transfers atom will return either an empty or non-empty sequence
   (swap! transfers
          (fn [tfers]
            ;;doall converts the lazy sequence returned from keep to a sequence that can be
            ;;checked by the empty? function
            (doall
             ;;includes any non-nil results from the items in the collection stored in the transfers atom
             ;;if non-nil item is found, it means the checkpoint upload is not complete or failed.
             (keep (fn [transfer]
                     (let [{:keys [key upload size-bytes start-time]} transfer
                           elapsed (- (System/nanoTime) start-time)]
                       (cond (> elapsed timeout-ns)
                             (throw (ex-info "Google Cloud Storage forcefully timed out by storage interface."
                                             {:timeout-ns (ns->ms timeout-ns)
                                              :elapsed-ms (ns->ms elapsed)}))

                             (future-cancelled? upload)
                             (throw (ex-info "Google Cloud Storage checkpoint was cancelled. This should never happen." {}))

                             (future-done? upload)
                             (let [{:keys [checkpoint-store-latency checkpoint-written-bytes]} monitoring]
                               (debug "Completed checkpoint to Google Cloud Storage under key" key)
                               (m/update-timer-ns! checkpoint-store-latency elapsed)
                               (.addAndGet ^AtomicLong checkpoint-written-bytes size-bytes)
                               ;;completed transfer needs to return nil
                               nil)

                             :else
                             transfer)))
                   tfers))))))

;;Cancels all current uploads in the checkpoint process.
;;Args: `onyx.storage.gcs.CheckpointManager`
;;Returns: nil valued transfers atom managed by the CheckpointManager
(defmethod onyx.checkpoint/cancel! onyx.storage.gcs.CheckpointManager 
  [{:keys [transfers]}]
  (run! #(future-cancel (:upload %)) @transfers)
  (reset! transfers nil))


;;Shuts down the api to Google Cloud Storage
;;Args: `onyx.storage.gcs.CheckpointManager`
;;Returns: void
(defmethod onyx.checkpoint/stop onyx.storage.gcs.CheckpointManager
  [chk-mgr]
  (onyx.checkpoint/cancel! chk-mgr))


(def max-read-checkpoint-retries 5)

(defn read-checkpointed-bytes [^Storage storage ^Bucket bucket ^String key]
  (let [_ (infof "Reading checkpointed bytes from bucket: %s" bucket)
        blob (get-blob bucket key)]
    (try (.getContent ^com.google.cloud.storage.Blob blob (make-array com.google.cloud.storage.Blob$BlobSourceOption 0))
         (catch Exception ex
           (error "Exception while trying to read checkpoint" ex)
           (throw (ex-info "Didn't read entire checkpoint."
                           {:message (.getMessage ex)
                            :exception ex}))))))

;;Reads a checkpoint back out of Google Cloud Storage
;;Args:
;;chk-mgr - An instance of `onyx.storage.gcs.CheckpointManager` record
;;tenancy-id -
;;job-id - The unique id of the job to which this belongs.
;;replica-version -
;;epoch -
;;task-id -
;;slot-id -
;;checkpoint-type - 
;;Returns: byte array read back from Google Cloud Storage Bucket
(defmethod onyx.checkpoint/read-checkpoint onyx.storage.gcs.CheckpointManager
  [{:keys [storage bucket id monitoring] :as chk-mgr}
   tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type]
  (let [k (checkpoint-task-key tenancy-id job-id replica-version epoch task-id slot-id checkpoint-type)]
    (loop [n-retries max-read-checkpoint-retries]
      (let [result (try (read-checkpointed-bytes storage bucket k)
                        (catch StorageException ex
                          ex))]
        (if (= (type result) com.google.cloud.storage.StorageException)
          (if (and (pos? n-retries)
                   (or
                    ;;Not Found
                    (= 404 (.getCode ^StorageException result))
                    ;;Gone
                    (= 410 (.getCode ^StorageException result))))
            (do
              (infof "Unable to read Google Cloud Storage checkpoint as the key, %s, does not exist yet. Retrying up to %s more times"
                     k n-retries)
              (LockSupport/parkNanos (* 1000 1000000))
              (recur (dec n-retries)))
            (throw result))
          (do
            (.addAndGet ^AtomicLong (:checkpoint-read-bytes monitoring) (alength ^bytes result))
            result))))))

