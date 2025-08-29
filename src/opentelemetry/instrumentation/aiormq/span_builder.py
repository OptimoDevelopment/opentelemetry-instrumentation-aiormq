# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
from typing import Optional

from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.semconv.trace import (MessagingOperationValues,
                                         SpanAttributes)
from opentelemetry.trace import Span, SpanKind, Tracer
from pamqp import commands as spec

from aiormq import Channel
from aiormq.abc import DeliveredMessage

_DEFAULT_ATTRIBUTES = {SpanAttributes.MESSAGING_SYSTEM: "rabbitmq"}


class SpanBuilder:
    def __init__(self, tracer: Tracer):
        self._tracer = tracer
        self._attributes = _DEFAULT_ATTRIBUTES.copy()
        self._operation: MessagingOperationValues = None
        self._kind: SpanKind = None
        self._destination: str = None
        self._properties: Optional[spec.Basic.Properties] = None

    def set_as_producer(self):
        self._kind = SpanKind.PRODUCER

    def set_as_consumer(self):
        self._kind = SpanKind.CONSUMER

    def set_operation(self, operation: MessagingOperationValues):
        self._operation = operation

    def set_destination(self, destination: str):
        self._destination = destination
        self._attributes[SpanAttributes.MESSAGING_DESTINATION] = destination

    def set_channel(self, channel: Channel):
        connection = channel.connection
        url = connection.url
        self._attributes.update(
            {
                SpanAttributes.NET_PEER_NAME: url.host,
                SpanAttributes.NET_PEER_PORT: url.port or 5672,
            }
        )

    def set_properties(self, properties: Optional[spec.Basic.Properties] = None):
        self._properties = properties
        if self._properties is None:
            return

        self._attributes["rabbitmq.headers"] = (
            json.dumps(properties.headers)
        )

        if self._properties.message_id:
            self._attributes[SpanAttributes.MESSAGING_MESSAGE_ID] = (
                self._properties.message_id
            )
        if self._properties.correlation_id:
            self._attributes[SpanAttributes.MESSAGING_CONVERSATION_ID] = (
                self._properties.correlation_id
            )

    def build(self) -> Optional[Span]:
        if not is_instrumentation_enabled():
            return None
        if self._operation:
            self._attributes[SpanAttributes.MESSAGING_OPERATION] = (
                self._operation.value
            )
        else:
            self._attributes[SpanAttributes.MESSAGING_TEMP_DESTINATION] = True
        span = self._tracer.start_span(
            self._generate_span_name(),
            kind=self._kind,
            attributes=self._attributes,
        )
        return span

    def _generate_span_name(self) -> str:
        operation_value = self._operation.value if self._operation else "send"
        return f"{self._destination} {operation_value}"
