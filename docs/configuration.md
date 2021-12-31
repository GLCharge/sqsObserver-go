# SQS configuration

## Understanding the internal API and configuration

### The `Observer` and `Publisher` interfaces

The Observer is an abstract interface that listens to one/multiple sources and will output any messages received to a
dedicated channel.

A `Publisher` is also an abstract interface, which consumes messages from a channel or gets a message from a `Send`
interface.

### The `ObserverManager`

The core component of the internal API is a `ObserverManager` that keeps track of all queues, their tags and enables the
central system to access queues at any given time. The manager also contains a default `Publisher` that will send
messages to the appropriate Queue based on the `messageType`.

### Configuration concepts

The configuration describes internal SQS observer configuration. It will group multiple `queue`s based on their tags and
add them to one `Observer`. This improves performance if the service gets a lot of requests from a single queue. If the
queue `Tag` is not specified explicitly, the queue will be added to a `DefaultObserver`, a round-robin queue
listener/`Observer`. The queue names/urls in this case are based on the `messageType`.

## Configuration file

```yaml
sqs:
  messageTimeout: 5
  pollDuration: 3
  queues:
    - tag: ""
      messageType: ""
      pollDuration: 10
    - tag: ""
      messageType: ""
      pollDuration: 10
```