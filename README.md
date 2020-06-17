# Command line Azure ServiceBus Explorer

Explore Azure Service Bus on command line. You can send, receive, peek message from topic/subscriptions.

## Installation

Requires python >= `3.7.4`

```bash
pip install asb-cli-explorer
```

## Quick start

Send a messge.

```bash
export SB_CONN_STR="Endpoint=sb://<full conn string having sender or manage role>"
asb-tour send --conn-str=${SB_CONN_STR} --topic=test-topic --props key1=va1,key2=value2 '{"hello":"world"}'

# using json file
asb-tour send --conn-str=${SB_CONN_STR} --topic=test-topic --props key1=va1,key2=value2 --data-file /path/to/payload_file
```

Peek/stream messge from a subscription asynchronously forever. Press 'Ctrl+C' to stop.

```bash
export SB_CONN_STR="Endpoint=sb://<full conn string having receiver or manage role>"
asb-tour peek --topic=test-topic --subscription=log --show-user-props --show-system-props

# optionaly pipe it to `jq` to get pretty printing and futher transformations
asb-tour peek --topic=test-topic --subscription=log --show-user-props --show-system-props | jq
```

List messages from subscription's dead letter queue (dlq).

```bash
export SB_CONN_STR="Endpoint=sb://<full conn string having receiver or manage role>"
asb-tour dlq peek --topic=test-topic --subscription=log --count=10
```

Move/Copy dlq messages to topics

```bash
export SB_CONN_STR="Endpoint=sb://<full conn string having receiver or manage role>"
asb-tour dlq move --topic=test-topic --subscription=log <message-ids>
asb-tour dlq copy --topic=test-topic --subscription=log <message-ids>
asb-tour dlq purge --topic=test-topic --subscription=log
```

## Explore Topics, Subscriptions: Messages & DLQ

You can also explore messages using Terminal User Interface [TUI].

```bash
export SB_CONN_STR="Endpoint=sb://<full conn string having manage role>"
asb-tour explore
```
