(defproject kda-sim "0.1.0-SNAPSHOT"
  :description "Simulator for AWS KDA"
  :url "https://github.com/jungflykim/kda-sim"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire/cheshire "5.9.0"]
                 [tea-time "1.0.1"]
                 [datascript "0.18.8"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.apache.logging.log4j/log4j-core "2.12.1"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.12.1"]
                 [software.amazon.kinesis/amazon-kinesis-client "2.2.4"]
                 [com.h2database/h2 "1.4.200"]
                 [honeysql "0.9.8"]
                 [seancorfield/next.jdbc "1.0.13"]
                 [camel-snake-kebab "0.4.1"]
                 ]
  :java-source-paths ["main/java"]
  :javac-options     ["-target" "1.8" "-source" "1.8"]
  :repl-options {:init-ns kda-sim.main}
  :main kda-sim.main/-main
  :aot :all)
