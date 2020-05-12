import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
import re
import logging
import sys
import os

PROJECT='peppy-freedom-276106'
schema = 'frame_time:STRING, frame_time_relative:STRING, ip_src:STRING, eth_src_resolved:STRING, ip_dst:STRING, eth_dst_resolved:STRING, frame_len:STRING, frame_protocols:STRING'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/media/knilesh/DATA/docs/Apollo's Landing-0424566883dc.json"

src_path = "sit.txt"

class Split(beam.DoFn):

    def process(self, element):
        element = element.split("\t")
        print(element)
        
        return [{ 
            'frame_time': element[0],
            'frame_time_relative': element[1],
            'ip_src': element[2],
            'eth_src_resolved': element[3],
            'ip_dst': element[4],
            'eth_dst_resolved': element[5],
            'frame_len': element[6],
            'frame_protocols': element[7]
        }]

def main():

   p = beam.Pipeline(options=PipelineOptions())

   (p
      | 'ReadData' >> beam.io.textio.ReadFromText(src_path)
      | 'ParseCSV' >> beam.ParDo(Split())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:packetcaptures.managed_wlp3s0'.format(PROJECT), schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   )

   p.run()

if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()