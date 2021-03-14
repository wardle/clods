(ns com.eldrix.clods.fhir-r4
  (:require [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.core :as clods])
  (:import (ca.uhn.fhir.rest.server RestfulServer IResourceProvider)
           (ca.uhn.fhir.context FhirContext)
           (org.hl7.fhir.r4.model Organization IdType)
           (ca.uhn.fhir.rest.annotation Read IdParam)
           (org.eclipse.jetty.servlet ServletContextHandler ServletHolder)
           (ca.uhn.fhir.rest.server.interceptor ResponseHighlighterInterceptor)
           (org.eclipse.jetty.server Server ServerConnector)
           (com.eldrix.clods.core ODS)
           (javax.servlet Servlet)))

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
    (doto (org.hl7.fhir.r4.model.Organization.)
      (.setId "1")
      (.setName "Test organisation"))))

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
  (def server (Server.))

  (def connector (ServerConnector. server))
  (.addConnector server (doto connector
                          (.setPort 8080)))
  (def handler (ServletContextHandler. ServletContextHandler/SESSIONS))
  (.setContextPath handler "/")
  (def servlet-holder (ServletHolder. r4-servlet))
  (.addServlet handler servlet-holder "/fhir/*")
  (.setHandler server handler)
  (.start server)
  (.stop server)
  )
