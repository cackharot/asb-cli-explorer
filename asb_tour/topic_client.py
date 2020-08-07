import os
from types import SimpleNamespace
import sys
from azure.servicebus._base_handler import _parse_conn_str
from azure.servicebus._control_client import ServiceBusService

class TopicClient(object):
    def __init__(self, conn_str):
        self._conn_str = conn_str
        (self._namespace, self._entity, self._bus) = self._parse()

    def _parse(self):
        host, policy, key, entity_in_conn_str = _parse_conn_str(self._conn_str)
        ns = host.split('.')[0]
        return (ns, entity_in_conn_str, ServiceBusService(
            service_namespace = ns,
            shared_access_key_name = policy,
            shared_access_key_value = key
        ))

    @property
    def namespace(self):
        return self._namespace

    def topics(self):
        topics = self._bus.list_topics() if not self._entity else [SimpleNamespace(name=self._entity)]
        tp_names = [x.name for x in topics]
        data = []
        for tp in topics:
            subs = self._bus.list_subscriptions(tp.name)
            # for x in subs:
            #     print(dir(x))
            s = [
                 SimpleNamespace(**dict(name=x.name, message_count=x.message_count))
                 for x in subs
            ]
            data.append((tp.name, s))
        return data
