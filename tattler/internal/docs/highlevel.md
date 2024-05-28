# Tattler 

## Introduction

Tattler is a set of packages that allow delivering information from the APIServer to any number of listening
services.

Initial design is targeted at Azure Graph Notification Service (AGN) to allow publishing of K8 node, pod and namespace objects. This may be expanded in the future to other objects.

The initial package is not going to integrate existing data pulls, but will add them later as their own custom readers and processors.

## Details

The packages are meant to allow reader objects to send a stream of objects into a pipeline that moves objects into differnt processors.

The only restriction to readers is they must by able to output the object as a `data.Entry`. To be stored in a `data.Entry`, you must implement a custom type inside the `data` package that can implement `SourceData`, which is defined as:

```go
// SourceData is a generic type for objects wrappers in this package.
type SourceData interface {
	// GetUID returns the UID of the object.
	GetUID() types.UID
	// Object returns the object as a runtime.Object. This is always for the latest change,
	// in the case that this is an update.
	Object() runtime.Object
}
```
This interface ensures your type is a K8 object and ensures a quick lookup of its UID which is used to remove duplicates during batching operations.

### Pipelining

These packages are setup to make a small pipeline. The pipeline processes objects sent by readers for date safety, batches them up and routes them to data processors.  

The data flow is as follows:

```
reader(s) -> safety.Secrets -> batching.Batcher -> routing.Batches -> data processors
```

- reader(s) are custom readers for various APIServer API calls that write to the input channel of safety.
- safety.Secrets looks into containers and redacts secrets that may have been passed in env variables
- batching.Batcher batches all input over some time period and sends it for routing to data processors. The batching time is universal.
- routing.Batches accepts batches of data from routing.Batches and sends the data to all registered data processors.
- data processors are custom data processors that pick through the batched data and do something with it.

### Adding an APIServer reader

Adding an APIServer reader is as simple as making a call to the APIServer and outputing the data to the safety.Secrets instance. You will need to modify the `data/` package in order to have support for your data. And you register your reader via the tattler instance that should be in your programs main.go file.

### Adding a data processor

Adding a data processor is as simple as writing one that can register an input channel with the `routing.Register()` method.

Note that if your data processor is slower that what it receives and has no buffer, data will be dropped. Scale your buffers appropriately for large clusters that on start might send things like 200K pods + other data types.

Do not hold onto data passed in, simply use it and let it expire to prevent memory leaks of large data.