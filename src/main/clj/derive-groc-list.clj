(ns getting-data.groc
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string]))
            

(def groc-data (with-open [in-file (io/reader "grocs.tsv")]
  (doall
    (csv/read-csv in-file))))

(def unique-products (distinct (mapcat #(clojure.string/split (first %) (re-pattern "\t")) groc-data)))