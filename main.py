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
import argparse
import logging
from datetime import datetime
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


class ProcessPubSubMessage(beam.DoFn):

    def process(self, element, sideInput, publish_time=beam.DoFn.TimestampParam):
        yield {
            'topic': sideInput['topic'],
            'data': element.decode('utf-8'),
            'created_at': datetime.utcfromtimestamp(
                float(publish_time)
            ).strftime('%Y-%m-%d %H:%M:%S.%f')
        }


class PrettyPrintMessage(beam.DoFn):

    def process(self, element, *args, **kwargs):
        logging.info(element)
        yield element


def run(inputTopic, outputTable, windowSize, pipelineArgs):
    """Build and run the pipeline."""
    pipeline_options = PipelineOptions(pipelineArgs, streaming=True, save_main_session=True)
    sideProperties = {'topic': inputTopic}

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub into a PCollection.
        (
            p
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(topic=inputTopic)
            | 'ProcessMessages' >> beam.ParDo(ProcessPubSubMessage(), sideProperties)
            | 'PrintToLog' >> beam.ParDo(PrettyPrintMessage())
            | "chunk messages" >> beam.WindowInto(
                window.FixedWindows(windowSize, 0),
                trigger=AfterProcessingTime(10),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=1800  # 30 minutes
            )
            | 'WriteToBigQuery' >> WriteToBigQuery(
                outputTable,
                schema={
                    'fields': [
                        {
                            "name": "created_at",
                            "mode": "NULLABLE",
                            "type": "TIMESTAMP"
                        },
                        {
                            "name": "topic",
                            "type": "STRING",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "data",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        }
                    ]
                },
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from.\n"
             '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--output_table",
        help=(
            'Output BigQuery table for results specified as: '
            '<project-id>:<dataset-id>.<table-id> or <dataset-id>.<table-id>'),
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in number of minutes.",
    )
    known_args, pipeline_args = parser.parse_known_args()
    run(
        inputTopic=known_args.input_topic,
        outputTable=known_args.output_table,
        windowSize=known_args.window_size,
        pipelineArgs=pipeline_args
    )
