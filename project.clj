(defproject clods "0.1.0-SNAPSHOT"
  :description "Organisational data services server and tooling"
  :url "https://github.com/wardle/clods"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [ring/ring-core "1.8.1"]
                 [ring/ring-jetty-adapter "1.8.1"]
                 [clj-time "0.14.2"]
                 [org.clojure/data.xml "0.2.0-alpha6"]
                 [org.clojure/data.zip "1.0.0"]
                 [camel-snake-kebab "0.4.1"]
                 [migratus "1.2.8"]
                 [org.postgresql/postgresql "42.2.12.jre7"]
                 [seancorfield/next.jdbc "1.0.445"]
                 [org.clojure/data.json "1.0.0"]
                 [clj-http "3.10.1"]
                 [clj-bom "0.1.2"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.clojure/tools.logging "1.1.0"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [cli-matic "0.3.11"]]
  :plugins [[lein-ring "0.12.5"]]
  :main com.eldrix.clods.main
  :aot [com.eldrix.clods.main]
  :ring {:handler com.eldrix.clods.service/app}
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.2"]]}})
