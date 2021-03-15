(ns com.eldrix.clods.fhir.r4.serve
  (:require [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.core :as clods]
            [com.eldrix.clods.fhir.r4.convert :as r4convert]
            [clojure.string :as str])
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


(definterface OrganizationGetResourceById
  (^org.hl7.fhir.r4.model.Organization getResourceById [^org.hl7.fhir.r4.model.IdType id]))

(deftype OrganizationResourceProvider [^ODS ods]
  IResourceProvider
  (getResourceType [_this] Organization)
  OrganizationGetResourceById
  (^{:tag org.hl7.fhir.r4.model.Organization
     Read true}                                             ;; The "@Read" annotation indicates that this method supports the read operation. It takes one argument, the Resource type being returned.
    getResourceById [_this ^{:tag    org.hl7.fhir.r4.model.IdType
                             IdParam true} id]
    (let [[value system] (reverse (str/split (.getIdPart id) #"\|"))
          _ (log/info "Fetch organisation " system "|" value)
          org (clods/fetch-org ods system value)
          _ (log/info "Result" org)]
      (if org
        (r4convert/make-organization org)
        (throw (ResourceNotFoundException. id))))))

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
  )
