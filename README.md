## onyx-rabbitmq

Onyx plugin for rabbitmq.

#### Installation

In your project file:

```clojure
[onyx-rabbitmq "0.7.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.rabbitmq])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/plugin :onyx.plugin.rabbitmq/input
 :onyx/type :input
 :onyx/medium :rabbitmq
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from rabbitmq"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.rabbitmq/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:rabbitmq/attr`            | `string`  | Description here.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
