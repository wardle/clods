(ns com.eldrix.clods.svc)

(defprotocol Store
  (get-code [this code] "Return the value for the ODS code specified.")
  (get-org [this id] "Return the organisation matching the id specified.")
  (get-postcode [this pc] "Return NHSPD data for the postcode.")
  (get-general-practitioner [this id] "Return information about a GP.")
  (get-general-practitioners-for-org [this id] "Return GPs in an organisation.")
  (search-org [this params] "Search for an organisation."))
