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
from typing import Collection

import wrapt
from opentelemetry import trace
from opentelemetry.instrumentation.aiormq.callback_decorator import \
    CallbackDecorator
from opentelemetry.instrumentation.aiormq.package import _instruments
from opentelemetry.instrumentation.aiormq.publish_decorator import \
    PublishDecorator
from opentelemetry.instrumentation.aiormq.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.trace import Tracer

from aiormq import Channel
from aiormq.abc import ConsumerCallback

_INSTRUMENTATION_MODULE_NAME = "opentelemetry.instrumentation.aiormq"


class AioRmqInstrumentor(BaseInstrumentor):
    @staticmethod
    def _instrument_consume(tracer: Tracer):
        async def wrapper(wrapped, instance, args, kwargs):
            async def basic_consume(
                    queue: str,
                    consumer_callback: ConsumerCallback,
                    *fargs,
                    **fkwargs,
            ):
                decorated_callback = CallbackDecorator(
                    tracer, instance
                ).decorate(consumer_callback)
                return await wrapped(queue=queue, consumer_callback=decorated_callback, *fargs, **fkwargs)

            return await basic_consume(*args, **kwargs)

        wrapt.wrap_function_wrapper(Channel, "basic_consume", wrapper)

    @staticmethod
    def _instrument_publish(tracer: Tracer):
        async def wrapper(wrapped, instance, args, kwargs):
            decorated_publish = PublishDecorator(tracer, instance).decorate(
                wrapped
            )
            return await decorated_publish(*args, **kwargs)

        wrapt.wrap_function_wrapper(Channel, "basic_publish", wrapper)

    def _instrument(self, **kwargs):
        tracer_provider = kwargs.get("tracer_provider", None)
        tracer = trace.get_tracer(
            _INSTRUMENTATION_MODULE_NAME,
            __version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )
        self._instrument_consume(tracer)
        self._instrument_publish(tracer)

    @staticmethod
    def _uninstrument_consume():
        unwrap(Channel, "basic_consume")

    @staticmethod
    def _uninstrument_publish():
        unwrap(Channel, "basic_publish")

    def _uninstrument(self, **kwargs):
        self._uninstrument_consume()
        self._uninstrument_publish()

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments
