# kafka-events-disk

Kafka consumer utility library that saves events on the 'work-unit-event-ch' channel to disk.

The work units will be written as JSON message and each message separated by a new line.

Each message is augmented with the keys

:host the-local-host-name

The ```fileape``` library is used to write the messages to local disk and roll them over.
By default the messages are rolled for every 10mb and an inactivity of 10 seconds, the messages
are also written compressed using gzip.


## Usage

```clojure

(require '[kafka-events-disk.core :as ed])

;;create a node from kafka-clj.consumer.consumer

(ed/register-writer! node {:path "/tmp/kafka-workunits"})
(ed/close-writer! node)

```

## License

Copyright © 2014 gerritjvv

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
