(ns com.eldrix.clods.import.core
  "Provides functions for importing data relevant to organisational data
  services including:

  - the organisational data services (ods) master XML file (TRUD)
  - data on current and archived general practitioners (NHS Digital)
  - the NHS postcode directory (nhspd) - (ONS)

   The complete range of organisational data is made up from multiple sources
   in different formats and 'clods' is designed to abstract and hide this complexity
   as much as possible. Sadly, as far as I am aware TRUD nor ONS geography do
   not provide an API to download data files or an easy way to support the
   automatic identification of updated files. To confuse matters, many of the
   downloads available are actually derived from a master XML file of
   organisational data or the canonical source is unclear.

   At the time of writing, the following sources of information can be used:

   1. The NHS Postcode directory (NHSPD) lists all current and terminated
   postcodes in the UK and relates them to a range of current statutory
   administrative, electoral, health and other geographies. Unfortunately,
   it is not possible to automatically download these data from a machine-
   readable canonical resource, as far as I know, but the download is
   available manually. e.g. Download the [February 2020 release](https://geoportal.statistics.gov.uk/datasets/nhs-postcode-directory-uk-full-february-2020)

   2. The [XML file from TRUD](https://isd.digital.nhs.uk/trud3/user/guest/group/0/pack/5/subpack/341/releases)
    has the most complete organisational data reference set

   3. But, this XML file must be supplemented with additional files for current
   GPs (egpcur) and archived GPs (egparc). These are listed as 'No' under
   'Available in XML' on the [GP and GP practice related web page](https://digital.nhs.uk/services/organisation-data-service/data-downloads/gp-and-gp-practice-related-data).
   This software can automatically download these additional files.

   The future goal is to provide an automated download and parsing functionality that
   could be run on schedule. This automated API will replace this. "
  (:require [com.eldrix.clods.import.ods :as ods]
            [com.eldrix.clods.import.nhsgp :as nhsgp]
            [com.eldrix.clods.import.nhspd :as nhspd]
            [clojure.core.async :as async]))

(defn import-postcodes
  "Import NHS postcode data (NHSPD) data from the input stream specified."
  [in]
  (let [ch (async/chan 1 (partition-all 5000))]
    (async/thread (nhspd/import-postcodes in ch))
    ch))

(defn import-general-practitioners
  "Downloads and imports general practitioners, streaming batches on the channel
  returned."
  []
  (nhsgp/stream-general-practitioners 5000))

(defn import-ods-xml
  "Imports from the NHS ODS XML data file, returning data in batches
  on the channel returned."
  [in]
  (ods/stream-organisations in 4 500))

(defn import-all-codes
  [in]
  (ods/all-codes in))

(comment
  (def nhspd "/Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv")
  (def postcodes (import-postcodes nhspd))
  (async/<!! postcodes)

  (def gps (import-general-practitioners))
  (async/<!! gps)

  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml")
  (def ods (import-ods-xml filename))
  (async/<!! ods)
  )