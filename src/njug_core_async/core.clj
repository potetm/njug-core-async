(ns njug-core-async.core
  (:require [clojure.core.async :as async :refer [chan go <! >! <!! >!!]]
            [njug-core-async.pipeline :as pipeline]))

(defn start-printing-process [in-c]
  (go
    (loop []
      (let [v (<! in-c)]
        (when-not (nil? v)
          (println v)
          (recur))))))

(defn ex-1 []
  (let [c (chan 1)]
    (start-printing-process c)
    (>!! c "So Say We All")
    (>!! c 33) ;; look ma! no typing!
    (>!! c "Roll a hard 6")
    (>!! c "FRAK")))

(defn start-shouting [out-c]
  (go
    (loop []
      (when (>! out-c "FRAK")
        (<! (async/timeout 1000))
        (recur)))))

(defn ex-2 []
  (let [c (chan 1)]
    (start-printing-process c)
    (start-shouting c)))

(defn read-from-queue []
  (<!! (async/timeout 500))
  {:id (rand-int 40)
   :ship-type "Viper Mark VII"
   :status :repairs-required})

(defn assess-damage [v]
  (<!! (async/timeout (rand-int 500)))
  (merge v {:to-be-repaired [:vertical-stabilizers
                             :starboard-missile-launcher]}))

(defn repair-ship [v]
  (let [time (reduce + (range (rand-int 100000000)))]
    (<!! (async/timeout (rand-int 10000)))
    (-> (merge v {:status :ready-to-dock, :time-to-repair time})
        (dissoc :to-be-repaired))))

(defn save-status-in-db [v]
  (<!! (async/timeout (rand-int 50)))
  v)

(defn dock-ship [v]
  (<!! (async/timeout (rand-int 500)))
  v)

(defn ex-3 []
  (loop []
    (let [v (read-from-queue)]
      (-> (assess-damage v)
          (repair-ship)
          (save-status-in-db)
          (dock-ship)
          (clojure.pprint/pprint)))
    (recur)))

(defn ex-4 []
  (dotimes [_ 60]
    (.start
      (proxy [Thread] []
        (run []
          (loop []
            (let [v (read-from-queue)]
              (-> (assess-damage v)
                  (repair-ship)
                  (save-status-in-db)
                  (dock-ship)
                  (clojure.pprint/pprint)))
            (recur)))))))

(defn ex-5 []
  (let [in (chan 5)
        err (chan 1)]
    (pipeline/pipeline
      [5 assess-damage
       40 repair-ship
       5 save-status-in-db
       5 dock-ship
       1 clojure.pprint/pprint]
      in
      err)
    (loop []
      (when (>!! in (read-from-queue))
        (recur)))))
