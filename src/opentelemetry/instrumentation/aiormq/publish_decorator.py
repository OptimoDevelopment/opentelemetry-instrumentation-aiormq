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
from typing import Callable, Optional

from opentelemetry import propagate, trace
from opentelemetry.instrumentation.aiormq.span_builder import SpanBuilder
from opentelemetry.trace import Span, Tracer

import aiormq
from aiormq import Channel
from aiormq.abc import DeliveredMessage


class PublishDecorator:
    def __init__(self, tracer: Tracer, channel: Channel):
        self._tracer = tracer
        self._channel = channel

    def _get_publish_span(
            self, message: DeliveredMessage, routing_key: str, exchange: str
    ) -> Optional[Span]:
        builder = SpanBuilder(self._tracer)
        builder.set_as_producer()
        builder.set_destination(f"{exchange},{routing_key}")
        builder.set_channel(self._channel)
        builder.set_message(message)
        return builder.build()

    def decorate(self, basic_publish: Callable) -> Callable:
        async def decorated_basic_publish(
                message: DeliveredMessage, routing_key: str, exchange: str, **kwargs
        ) -> Optional[aiormq.abc.ConfirmationFrameType]:
            span = self._get_publish_span(messgae=message, routing_key=routing_key, exchange=exchange)
            if not span:
                return await basic_publish(message, routing_key, **kwargs)
            with trace.use_span(span, end_on_exit=True):
                if message.header:
                    properties = message.header.properties
                    propagate.inject(properties.headers)

                return_value = await basic_publish(message=message, routing_key=routing_key, exchange=exchange, **kwargs)
            return return_value

        return decorated_basic_publish
