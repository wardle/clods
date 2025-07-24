(ns com.eldrix.clods.fhir.r4.serve
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.core :as clods]
            [com.eldrix.clods.fhir.r4.convert :as r4convert])
  (:import (ca.uhn.fhir.rest.server RestfulServer IResourceProvider)
           (ca.uhn.fhir.context FhirContext)
           (org.hl7.fhir.r4.model Organization IdType Address Identifier)
           (ca.uhn.fhir.rest.annotation Read IdParam Search RequiredParam OptionalParam)
           (org.eclipse.jetty.servlet ServletContextHandler ServletHolder)
           (ca.uhn.fhir.rest.server.interceptor ResponseHighlighterInterceptor)
           (org.eclipse.jetty.server Server ServerConnector)
           (com.eldrix.clods.core ODS)
           (javax.servlet Servlet)
           (ca.uhn.fhir.rest.server.exceptions ResourceNotFoundException)
           (ca.uhn.fhir.rest.param InternalCodingDt)))



(defn parse-internal-coding-dt [^InternalCodingDt icdt]
  {:system (.getValueAsString (.getSystemElement icdt))
   :value  (.getValueAsString (.getCodeElement icdt))})

;;
;; we have to define interfaces for `deftype` to be able to implement them
;;

(definterface OrganizationGetResourceById
  (^org.hl7.fhir.r4.model.Organization getResourceById [^org.hl7.fhir.r4.model.IdType id]))

(definterface OrganizationSearch
  (^java.util.List searchByIdentifier [^ca.uhn.fhir.rest.param.TokenParam id])
  (^java.util.List search [^ca.uhn.fhir.rest.param.StringParam name
                           ^ca.uhn.fhir.rest.param.StringParam address
                           ^ca.uhn.fhir.rest.param.TokenOrListParam org-type]))

(deftype OrganizationResourceProvider [^ODS ods]
  IResourceProvider
  (getResourceType [_this] Organization)
  OrganizationGetResourceById
  (^{:tag org.hl7.fhir.r4.model.Organization
     Read true}                                             ;; The "@Read" annotation indicates that this method supports the read operation. It takes one argument, the Resource type being returned.
    getResourceById [_this ^{:tag    org.hl7.fhir.r4.model.IdType
                             IdParam true} id]
    (let [[value system] (reverse (str/split (.getIdPart id) #"\|"))
          root (when system (get r4convert/fhir-system->oid system))
          org (clods/fetch-org ods root value)]
      (if org
        (r4convert/make-organization ods org)
        (throw (ResourceNotFoundException. id)))))
  OrganizationSearch
  (^{:tag java.util.List Search true}
    searchByIdentifier [_this
                        ^{:tag ca.uhn.fhir.rest.param.TokenParam RequiredParam {:name "identifier"}} id]
    (let [root (get r4convert/fhir-system->oid (.getSystem id))
          extension (.getValue id)
          org (clods/fetch-org ods root extension)]
      (if org
        [(r4convert/make-organization ods org)]
        (throw (ResourceNotFoundException. (str id))))))
  (^{:tag java.util.List Search true}
    search [_this
            ^{:tag ca.uhn.fhir.rest.param.StringParam OptionalParam {:name "name"}} org-name
            ^{:tag ca.uhn.fhir.rest.param.StringParam OptionalParam {:name "address"}} address
            ^{:tag ca.uhn.fhir.rest.param.TokenOrListParam OptionalParam {:name "type"}} org-types]
    (if-not (or org-name address org-types)
      (throw (ResourceNotFoundException. "no search parameters."))
      (let [roles (when org-types (->> (.getListAsCodings org-types)
                                       (map parse-internal-coding-dt)
                                       (map #(if (or (= "2.16.840.1.113883.2.1.3.2.4.17.507" (:system %)) (= "urn:oid:2.16.840.1.113883.2.1.3.2.4.17.507" (:system %))) (:value %) "XXX"))))
            params (cond-> {}
                           org-name
                           (assoc :n (.getValue org-name))
                           address
                           (assoc :address (.getValue address))
                           roles
                           (assoc :roles roles))]
        (if-not (seq params)
          (throw (ResourceNotFoundException. "no supported search parameters."))
          (do
            (map (partial r4convert/make-organization ods) (clods/search-org ods params))))))))

(defn ^Servlet make-r4-servlet [^ODS ods]
  (proxy [RestfulServer] [(FhirContext/forR4)]
    (initialize []
      (log/info "Initialising HL7 FHIR R4 server; providers: Organization")
      (.setResourceProviders this [(OrganizationResourceProvider. ods)])
      (println (seq (.getResourceProviders this)))
      (.registerInterceptor this (ResponseHighlighterInterceptor.)))))

(defn ^Server make-server [^ODS ods {:keys [port]}]
  (let [servlet-holder (ServletHolder. (make-r4-servlet ods))
        handler (doto (ServletContextHandler. ServletContextHandler/SESSIONS)
                  (.setContextPath "/")
                  (.addServlet servlet-holder "/fhir/*"))
        server (doto (Server.)
                 (.setHandler handler))
        connector (doto (ServerConnector. server)
                    (.setPort (or port 8080)))]
    (.addConnector server connector)
    server))

(defn -main [& args]
  (if-not (= 3 (count args))
    (do (println "Usage: clj -M:fhir-r4 <ods-path> <nhspd-path> <port>")
        (System/exit 1))
    (let [[ods-path nhspd-path port-str] args
          port (Integer/parseInt port-str)
          ods (clods/open-index {:f ods-path :nhspd-file nhspd-path})
          server (make-server ods {:port port})]
      (.start server))))

(comment
  (def ods (clods/open-index "/var/tmp/ods" "/var/tmp/nhspd-nov-2020"))
  (def server (make-server ods {:port 8080}))
  (.start server)
  (.stop server))

