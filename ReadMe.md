# SQSObserver-go

This is an internal library used for observing (polling) SQS queues. OOP, easily configurable and running in goroutines.

Read the [configuration](docs/configuration.md) for queue configuration examples.

## Possible improvements

1. Worker pool and priority queues:
    - assign a priority for a queue
    - have a limited amount of workers in a pool
    - workers poll the queues based on the priority and switch between queues
2. Api changes (semantics)