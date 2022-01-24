(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.eldrix/clods)
(def version (format "1.0.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")

(def basis
  "Basis for a simple library jar file, or utility uberjar."
  (b/create-basis {:project "deps.edn"}))

(def http-server-basis
  "Basis for a runnable uberjar for a HTTP server."
  (b/create-basis {:project "deps.edn"
                   :aliases [:serve]}))
(def r4-server-basis
  "Basis for a runnable uberjar for a FHIR R4 server."
  (b/create-basis {:project "deps.edn"
                   :aliases [:fhir-r4]}))

(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def uberjar-file (format "target/%s-full-%s.jar" (name lib) version))
(def http-server-file (format "target/%s-http-server-%s.jar" (name lib) version))
(def r4-server-file (format "target/%s-fhir-r4-server-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar
  "Create a library jar file."
  [_]
  (clean nil)
  (println "Building   :" lib version)
  (b/write-pom {:class-dir class-dir
                :lib       lib
                :version   version
                :basis     basis
                :src-dirs  ["src"]
                :scm       {:url                 "https://github.com/wardle/clods"
                            :tag                 (str "v" version)
                            :connection          "scm:git:git://github.com/wardle/clods.git"
                            :developerConnection "scm:git:ssh://git@github.com/wardle/clods.git"}})
  (b/copy-dir {:src-dirs   ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file  jar-file}))


(defn install
  "Install library to local maven repository."
  [_]
  (clean nil)
  (jar nil)
  (println "Installing   :" lib version)
  (b/install {:basis     basis
              :lib       lib
              :version   version
              :jar-file  jar-file
              :class-dir class-dir}))

(defn deploy
  "Deploy library to clojars.
  Environment variables CLOJARS_USERNAME and CLOJARS_PASSWORD must be set."
  [_]
  (jar nil)
  (println "Deploying    :" lib version)
  (dd/deploy {:installer :remote
              :artifact  jar-file
              :pom-file  (b/pom-path {:lib       lib
                                      :class-dir class-dir})}))

(defn http-server
  "Build an runnable HTTP server uberjar file."
  [_]
  (clean nil)
  (b/copy-dir {:src-dirs   ["src" "serve"]
               :target-dir class-dir})
  (b/compile-clj {:basis      http-server-basis
                  :src-dirs   ["src" "serve"]
                  :ns-compile ['com.eldrix.clods.serve]
                  :class-dir  class-dir})
  (b/uber {:class-dir class-dir
           :uber-file http-server-file
           :basis     http-server-basis
           :main      'com.eldrix.clods.serve}))

(defn fhir-r4-server
  "Build an runnable FHIR R4 server uberjar file."
  [_]
  (clean nil)
  (b/copy-dir {:src-dirs   ["src" "fhir_r4"]
               :target-dir class-dir})
  (b/compile-clj {:basis      r4-server-basis
                  :src-dirs   ["src" "fhir_r4"]
                  :ns-compile ['com.eldrix.clods.fhir.r4.serve]
                  :class-dir  class-dir})
  (b/uber {:class-dir class-dir
           :uber-file r4-server-file
           :basis     r4-server-basis
           :main      'com.eldrix.clods.fhir.r4.serve}))