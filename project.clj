(defproject kda-sim "0.1.0-SNAPSHOT"
  :description "Simulator for AWS KDA"
  :url "https://github.com/jungflykim/kda-sim"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire/cheshire "5.9.0"]]
  :java-source-paths ["main/java"]
  :javac-options     ["-target" "1.8" "-source" "1.8"]
  :repl-options {:init-ns kda-sim.core}
  :main kda-sim.core/-main
  :aot :all)
