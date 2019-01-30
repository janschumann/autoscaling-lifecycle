# Autoscaling Lifecycle

A library to handle aws events.

This library is based on the [transitions](https://github.com/pytransitions/transitions) state machine implementation.

## Base concepts

### Event

This library can handle the following AWS events:
* Autoscaling events (aws.autoscaling)
* EC2 Command Status-change Notification events (aws.ssm)
* Scheduled events (aws.events)

To use this library, you need to implement it in a lambda function that is configured as a target of a cloudwatch rule
which collects events of those types.

Each event is than treated as an attempt to transition a resource from its `current state` to a specific `destination
state`. 

### Model

The model contains a configuration of possible transitions and implements the corresponding tasks to actually perform 
the transitions. It takes an event as a parameter from which it loads the resource to operate on and gets initialized 
with the current state of that resource. 

It will be notified of any state changes that are performed and is responsible to persist the resulting state back to 
the underlying resource.

#### Base implemenation for handling autoscaling lifecycle events

@todo create new class LifecycleModel (based on Model) and move lifecycle specific methods to the new class 

This library contains an abstract implementation of a model which focuses on autoscaling lifecycle events. To make 
autoscaling instances available to this model, they need to be registered to a dynamodb table containing the following
fields: 
* Ident: the instance id
* ItemStatus: `new` or `pending` 
* InstanceIp: the private ip of that instance  

If an instance is `new` or `pending`, the model will wait for it to reach the initial state (the very first source 
state in the configuration). Usually both, registering a node and updating the state (ItemStatus) to the initial state 
is done through a cloud init script. In that case the initial state might be `finished_cloud_init` which is set in the 
very last line of the cloud init script. 

Once a new instance has reached its initial state or an existing node has been loaded, the model initialisation 
is complete and can now be used by the `LifecycleHandler`.

As a base implementation, this model already provides tasks 

A common configuration of an autoscaling model might contain the following transitions: 
* finished_cloud_init => join_cluster
* join_cluster => joined_cluster
* joined_cluster => updating_dns
* updating_dns => dns_updated
* dns_updated => completing_lifecycle
* completing_lifecycle => ready

 

Please subclass this model and add a transition configuration by implementing `Model.get_transitions()` and the 
corresponding task methods.  

### LifecycleHandler

The LifecycleHandler is the heart of this library. It initializes a new state machine using the transitions it gets 
from a model and dispatches the corresponding triggers for the current state.  

@todo document failure handling and stop conditions
@todo rename to Dispatcher

## Configuration

A transition is configured as a `dict` with the following format:
```
{
    'source': 'state1',
    'dest': 'state2',
    'triggers': [
        {
            'name': 'do_something',
        }
    ]
}
``` 
This means: To transition a resource from `state1` to `state2`, the trigger `do_something` needs to be called.

A trigger is a list of tasks (methods implemented by the model). These tasks may contain conditions and can be
configured to run `before` or `after` (or both) the state change gets persisted to the model.  
```
{
    'source': 'state1',
    'dest': 'state2',
    'triggers': [
        {
            'name': 'do_something',
            'conditions': [<method_in_model>],
            'after': [<method_in_model>],
            'before': [<method_in_model>],
        }
    ]
}
``` 


