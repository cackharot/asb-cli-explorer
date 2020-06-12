# Command line Azure ServiceBus Explorer

Explore Azure Service Bus on command line. You can send, receive, peek message from topic/subscriptions.

## Quick start

Send a messge.

```bash
asb-tour send --topic=test-topic key1=va1,key2=value2 '{"hello":"world"}'
```

Peek/stream messge from a subscription asynchronously forever.

```bash
asb-tour peek --topic test-topic --subscription log --show-user-props

# optionaly pipe it to `jq` to get pretty printing and futher transformations
asb-tour peek --topic test-topic --subscription log --show-user-props | jq
```
