(ns com.eldrix.clods.import.core
  "Provides functions for importing data relevant to organisational data
  services including:

  - the organisational data services (ods) master XML file
  - data on current and archived general practitioners (NHS Digital)
  - the NHS postcode directory (nhspd) - (ONS)

   The complete range of organisational data is made up from multiple sources
   in different formats and 'clods' is designed to abstract and hide this complexity
   as much as possible.

   At the time of writing, the following sources of information can be used:

   1. The NHS Postcode directory (NHSPD) lists all current and terminated
   postcodes in the UK and relates them to a range of current statutory
   administrative, electoral, health and other geographies. Unfortunately,
   it is not possible to automatically download these data from a machine-
   readable canonical resource, as far as I know, but the download is
   available manually. e.g. Download the [February 2020 release](https://geoportal.statistics.gov.uk/datasets/nhs-postcode-directory-uk-full-february-2020)

   2. The [XML file from TRUD](https://isd.digital.nhs.uk/trud3/user/guest/group/0/pack/5/subpack/341/releases)
    has the most complete organisational data reference set. This is TRUD subpack 341.

   3. But, this XML file must be supplemented with additional files for current
   GPs (egpcur) and archived GPs (egparc). These are listed as 'No' under
   'Available in XML' on the [GP and GP practice related web page](https://digital.nhs.uk/services/organisation-data-service/data-downloads/gp-and-gp-practice-related-data).
   Current GPs are included in TRUD subpack 58 but archived GPs are not. We
   therefore download direct from NHS digital."
  (:require [com.eldrix.clods.import.nhsgp :as nhsgp]
            [com.eldrix.clods.import.nhspd :as nhspd]
            [com.eldrix.trud.core :as trud]
            [com.eldrix.trud.zip :as trudz]
            [clojure.core.async :as async]
            [clojure.tools.logging.readable :as log])
  (:import (java.time LocalDate)))

(defn stream-postcodes
  "Stream NHS postcode data (NHSPD) data from the input stream specified."
  [in]
  (let [ch (async/chan 1 (partition-all 5000))]
    (async/thread (nhspd/import-postcodes in ch))
    ch))

(defn stream-general-practitioners
  "Downloads and imports general practitioners, streaming batches on the channel
  returned."
  []
  (nhsgp/stream-general-practitioners 5000))

(defn do-batch-count
  "Drain a channel of batches of data, counting the total and running `f` for each batch."
  [ch f]
  (async/<!!
    (async/reduce
      (fn [total batch]
        (f batch)
        (+ total (count batch)))
      0 ch)))

(defn download-ods-xml
  "Downloads the latest ODS distribution file directly from UK TRUD.
  We can use the TRUD tooling to automatically download the release (341).
  If last-update is current, download will be skipped.
  This file *should* contain two nested zip files:
   - archive.zip
   - fullfile.zip
  Returns a map with the following keys:
   - release   - data directly from the TRUD API
   - paths     - all downloaded paths
   - xml-files - sequence of `java.nio.file.Path`s to the uncompressed XML files."
  ([api-key cache-dir] (download-ods-xml api-key cache-dir nil))
  ([api-key cache-dir ^LocalDate last-update]
   (let [latest (trud/get-latest api-key cache-dir 341 last-update)]
     (if-not (:needsUpdate? latest)
       (log/info "Skipping download ODS XML distribution files. Already up-to-date")
       (do (log/info "Successfully downloaded ODS XML distribution files." latest)
           (let [all-files (trudz/unzip2 [(:archiveFilePath latest)
                                          ["archive.zip" #"\w+.xml"]
                                          ["fullfile.zip" #"\w+.xml"]])]
             {:release   latest
              :paths     all-files
              :xml-files (concat (get-in all-files [1 1])
                                 (get-in all-files [2 1]))}))))))

(comment
  ;; import NHSPD from a file on disk
  (def nhspd "/Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv")
  (def postcodes (stream-postcodes nhspd))
  (async/<!! postcodes)

  ;; use the NHS TRUD service to download the files we need for NHS ODS XML  (distribution 341)
  (def api-key (slurp "/Users/mark/Dev/trud/api-key.txt"))
  (def xml-files (download-ods-xml api-key "/tmp/trud"))

  ;; download GPs direct from NHS digital
  (def gps (stream-general-practitioners))
  (async/<!! gps)

  )