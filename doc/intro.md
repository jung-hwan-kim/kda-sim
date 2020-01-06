# Introduction to kda-sim

(send-vehicle default-kinesis)
(send-heartbeat-k default-kinesis "0" true "kstate")
(let [stream-name "ds-prototype-raw"
      event {:EVENTTABLE "HEARTBEAT_K"
             :VEHICLE_ID "0"
             :DEBUG true
             :ACTION "kstate"
             :EVENTTIMESTAMP (EventtimestampParser/generateEventtimestampString)
             }
      ]
  (aws/kinesis-put stream-name [event])