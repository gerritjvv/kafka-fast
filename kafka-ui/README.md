# kafka-ui

Very basic kafka-ui

## Prerequisites

You will need [Leiningen][1] 1.7.0 or above installed.

[1]: https://github.com/technomancy/leiningen

## Configuration

Create the file: /etc/kafka-ui/conf.clj

e.g.

```clojure
;edn configuration file
;see https://github.com/edn-format/edn
{
:redis "redis server"
:kafka-brokers [{:host "kafka-broker" :port 9092}]

:topics ["test123" "ping"]
}
```

## Running

To start a web server for the application, run:

    lein ring server


## Contact

Email: gerritjvv@gmail.com

Twitter: @gerrit_jvv

## License


Distributed under the Eclipse Public License either version 1.0
