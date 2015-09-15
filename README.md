## onyx-rabbitmq

WIP - Onyx plugin for rabbitmq.

#### Installation

At this stage, `git clone` and `lein install`. Then in your project file:

```clojure
[onyx-rabbitmq "0.7.0-SNAPSHOT"]
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :in
 :onyx/plugin :onyx.plugin.rabbitmq-input/input
 :onyx/type :input
 :onyx/medium :rabbitmq
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :rabbit/queue-name test-queue-name
 :rabbit/host rabbitmq-host
 :rabbit/port rabbitmq-port
 :rabbit/key rabbitmq-key
 :rabbit/crt rabbitmq-crt
 :rabbit/ca-crt rabbitmq-ca-crt
 :rabbit/deserializer rabbitmq-deserializer
 :onyx/doc "Read segments from rabbitmq queue"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :in
  :lifecycle/calls :onyx.plugin.rabbitmq-input/reader-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:rabbitmq/queue-name`        | `string`  | The queue to read from/write to
|`::rabbit/host`               | `string`  | RabbitMQ host to connect to
|`::rabbit/port`               | `string`  | RabbitMQ port
|`::rabbit/key`                | `string`  | Path to private key for RabbitMQ TLS connection
|`::rabbit/crt`                | `string`  | Path to client cert for RabbitMQ TLS connection
|`::rabbit/ca-crt`             | `string`  | Path to CA cert for RabbitMQ TLS connection
|`::rabbit/deserializer`       | `keyword` | Function to use when deserializing messages from the queue. Takes bytes and returns a string

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015

Distributed under the Eclipse Public License, the same as Clojure.
