(ns njug-core-async.pipeline
  (:require [clojure.core.async :as a :refer [chan go go-loop <! >! <!! >!!]]
            [clojure.core.async.impl.protocols :as impl]))

(defn- try-map [f v err]
  (try
    (f v)
    (catch Throwable t
      (>!! err {:exception t, :mapping-function f, :value v})
      ::exception)))

(defn- go-pipe [in out]
  (go-loop []
    (when-some [val (<! in)]
      (>! out val)
      (recur))))

(defn- go-map [in f out err]
  (go-loop []
    (when-some [v (<! in)]
      (let [v (try-map f v err)]
        (cond
          (nil? v) (recur)
          (= ::exception v) (recur)
          (extends? impl/ReadPort (class v)) (do (<! (go-pipe v out))
                                                 (recur))
          :else
          (do (>! out v)
              (recur)))))))

(defn pipeline [desc c err]
  (let [p (partition 2 desc)]
    (reduce
      (fn [prev-c [n f]]
        (let [out-c (chan n)
              procs (for [_ (range n)]
                      (go-map prev-c f out-c err))
              close-chan (a/merge procs)]
          (a/take! close-chan (fn [_] (a/close! out-c)))
          out-c))
      c
      p)))

