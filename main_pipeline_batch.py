import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
import re
import logging
import sys
import os

PROJECT='peppy-freedom-276106'
schema = 'frame_number:INTEGER, frame_time_relative:STRING, eth_src_resolved:STRING,eth_dst_resolved:STRING, frame_len:INTEGER, frame_protocols:STRING'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/media/knilesh/DATA/docs/Apollo's Landing-0424566883dc.json"

src_path = "dolor.txt"

def regex_clean(data):
    return data


class Split(beam.DoFn):

    def process(self, element):
        element = element.split(",")

        return [{ 
            'frame_number': element[0],
            'frame_time_relative': element[1],
            'eth_src_resolved': element[2],
            'eth_dst_resolved': element[3],
            'frame_len': element[4],
            'frame_protocols': element[5]
        }]

def main():

   p = beam.Pipeline(options=PipelineOptions())

   (p
      | 'ReadData' >> beam.io.textio.ReadFromText(src_path)
      | "clean address" >> beam.Map(regex_clean)
      | 'ParseCSV' >> beam.ParDo(Split())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:packetcaptures.managed_wlp3s0'.format(PROJECT), schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   )

   p.run()

if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()