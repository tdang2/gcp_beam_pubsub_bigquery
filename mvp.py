#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
from datetime import datetime
import json


import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


class ProcessPubSubMessage(beam.DoFn):

    def process(self, element, *args, **kwargs):
        res = beam.Row(
            user=element['actor'],
            action=element['action'],
            created_at=datetime.fromisoformat(element['created_at']).strftime('%Y-%m-%d %H:%M:%S.%f')
        )
        yield res


class PrettyPrintMessage(beam.DoFn):

    def process(self, element, *args, **kwargs):
        logging.info(element)
        yield element


def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_table',
        required=True,
        help=(
            'Output BigQuery table for results specified as: '
            '<project-id>:<dataset-id>.<table-id> or <dataset-id>.<table-id>')
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--input_topic',
        help='Input PubSub topic of the form projects/<project-id>/topics/<topic-id>'
    )
    group.add_argument(
        '--input_subscription',
        help=(
          'Input PubSub subscription of the form '
          '"projects/<project-id>/subscriptions/<subscription-id>"'))
    known_args, pipeline_args = parser.parse_known_args(argv)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub into a PCollection.
        if known_args.input_subscription:
            messages = (
                p
                | beam.io.ReadFromPubSub(subscription=known_args.input_subscription).
                with_output_types(bytes)
            )
        else:
            messages = (
                p
                | beam.io.ReadFromPubSub(topic=known_args.input_topic).
                with_output_types(bytes)
            )
        output = (
            messages
            | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
            | beam.WindowInto(
                window.FixedWindows(10, 0),
                trigger=AfterProcessingTime(10),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=1800  # 30 minutes
            )
            | 'Parse Json' >> beam.Map(json.loads)
            | 'process' >> beam.ParDo(ProcessPubSubMessage())
        )

        # Streaming analytic here. Show this second
        aggregateRes = (
            output
            | 'PairWIthOne' >> beam.Map(lambda row: (row.user, 1,))
            | beam.CombinePerKey(sum)
            | 'Aggregate Per Window' >> beam.Map(
                lambda row,
                window=beam.DoFn.WindowParam: {
                    'event': row[0],
                    'event_count': row[1],
                    'window_batch': window.start.to_utc_datetime().strftime("%H:%m:%S") + '-' + window.end.to_utc_datetime().strftime("%H:%m:%S")
                }
             )
            | 'Convert To Pair KeyValue' >> beam.Map(lambda x: (
                x['window_batch'],
                beam.Row(
                    event=str(x['event']),
                    event_count=int(x['event_count'])
                )
              ))
            | 'Aggregate per Batch' >> beam.GroupByKey()
        )
        aggregateRes | 'Print' >> beam.ParDo(PrettyPrintMessage())

        # Write to BigQuery.
        bqOutput = output | 'ConvertToDictionary' >> beam.Map(
            lambda x: {
                'user': x.user,
                'action': x.action,
                'created_at': x.created_at
            }
        )
        bqOutput | WriteToBigQuery(
            known_args.output_table,
            schema={
                'fields': [
                    {
                        "mode": "NULLABLE",
                        "name": "created_at",
                        "type": "TIMESTAMP"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "user",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "action",
                        "type": "STRING"
                    }
                ]
            },
            write_disposition=BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
