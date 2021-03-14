(ns com.eldrix.clods.fhir-r4
  (:require [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.core :as clods])
  (:import (ca.uhn.fhir.rest.server RestfulServer IResourceProvider)
           (ca.uhn.fhir.context FhirContext)
           (org.hl7.fhir.r4.model Organization IdType Address Identifier)
           (ca.uhn.fhir.rest.annotation Read IdParam)
           (org.eclipse.jetty.servlet ServletContextHandler ServletHolder)
           (ca.uhn.fhir.rest.server.interceptor ResponseHighlighterInterceptor)
           (org.eclipse.jetty.server Server ServerConnector)
           (com.eldrix.clods.core ODS)
           (javax.servlet Servlet)
           (ca.uhn.fhir.rest.server.exceptions ResourceNotFoundException)))

;; TODO: complete
(defn ^Address make-address [org]
  (doto (Address.)
    (.setPostalCode (get-in org [:location :postcode]))))

(defn make-identifiers [org]
  (let [root (get-in org [:orgId :root])
        extension (get-in org [:orgId :extension])
        ;; an OID is a legacy HL7 identifier - but the native ODS identifier
        oid (doto (Identifier.)
              (.setSystem root)
              (.setId extension))]
    (cond-> [oid]
            ;; Organisations
            (= :RC1 (:orgRecordClass org))
            (conj (doto (Identifier.) (.setSystem "https://fhir.nhs.uk/Id/ods-organization-code") (.setId extension)))
            ;; Organisation sites
            (= :RC2 (:orgRecordClass org))
            (conj (doto (Identifier.) (.setSystem "https://fhir.nhs.uk/Id/ods-site-code") (.setId extension))))))

(defn ^Organization make-organization [org]
  (doto (org.hl7.fhir.r4.model.Organization.)
    (.setId (get-in org [:orgId :extension]))
    (.setIdentifier (make-identifiers org))
    (.setActive (:active org))
    (.setAddress [(make-address org)])
    (.setName (:name org))))


(definterface OrganizationGetResourceById
  (^org.hl7.fhir.r4.model.Organization getResourceById [^org.hl7.fhir.r4.model.IdType id]))

(deftype OrganizationResourceProvider [^ODS ods]
  IResourceProvider
  (getResourceType [_this] org.hl7.fhir.r4.model.Organization)
  OrganizationGetResourceById
  (^{:tag org.hl7.fhir.r4.model.Organization
     Read true}                                             ;; The "@Read" annotation indicates that this method supports the read operation. It takes one argument, the Resource type being returned.
    getResourceById [this ^{:tag    org.hl7.fhir.r4.model.IdType
                            IdParam true} id]
    (let [base-url (.getBaseUrl id)
          identifier (.getIdPart id)
          org (clods/fetch-org ods base-url identifier)]
      (log/info "Fetch organisation by id: " base-url " id:" identifier)
      (log/info "Result " org)
      (if-not org
        (throw (ResourceNotFoundException. id))
        (make-organization org)))))

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
          ods (clods/open-index ods-path nhspd-path)
          server (make-server ods {:port port})]
      (.start server))))

(comment
  (def ods (clods/open-index "/var/tmp/ods" "/var/tmp/nhspd-nov-2020"))
  (def server (make-server ods {:port 8080}))
  (.start server)
  (.stop server)
  (clods/fetch-org ods nil "RWMBV")
  )
