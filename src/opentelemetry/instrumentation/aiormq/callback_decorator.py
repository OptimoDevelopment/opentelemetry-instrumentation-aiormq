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
from typing import Any, Callable, Optional

from opentelemetry import context, propagate, trace
from opentelemetry.instrumentation.aiormq.span_builder import SpanBuilder
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.semconv.trace import MessagingOperationValues
from opentelemetry.trace import Span, Tracer

from aiormq import Channel
from aiormq.abc import ConsumerCallback, DeliveredMessage


class CallbackDecorator:
    def __init__(self, tracer: Tracer, channel: Channel):
        self._tracer = tracer
        self._channel = channel

    def _get_span(self, message: DeliveredMessage) -> Optional[Span]:
        builder = SpanBuilder(self._tracer)
        builder.set_as_consumer()
        builder.set_operation(MessagingOperationValues.RECEIVE)
        builder.set_destination(message.exchange or message.routing_key)
        builder.set_channel(self._channel)
        builder.set_message(message)
        return builder.build()

    def decorate(
        self, callback: ConsumerCallback
    ) -> ConsumerCallback:
        async def decorated(message: DeliveredMessage):
            if not is_instrumentation_enabled():
                return await callback(message)

            headers = {}
            if message.header is not None:
                properties = message.header.properties
                headers = properties.headers or {}

            ctx = propagate.extract(headers)
            token = context.attach(ctx)
            span = self._get_span(message)
            if not span:
                return await callback(message)
            try:
                with trace.use_span(span, end_on_exit=True):
                    return_value = await callback(message)
            finally:
                if token:
                    context.detach(token)
            return return_value

        return decorated
