(defproject kda-sim "0.1.0-SNAPSHOT"
  :description "Simulator for AWS KDA"
  :url "https://github.com/jungflykim/kda-sim"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire/cheshire "5.9.0"]
                 [tea-time "1.0.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.apache.logging.log4j/log4j-core "2.12.1"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.12.1"]
                 [software.amazon.kinesis/amazon-kinesis-client "2.2.4"]
                 ]
  :java-source-paths ["main/java"]
  :javac-options     ["-target" "1.8" "-source" "1.8"]
  :repl-options {:init-ns kda-sim.core}
  :main kda-sim.core/-main
  :aot :all)
