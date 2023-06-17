import apache_beam as beam

input_file = 'gs://gcp-etl-csv-files/inventory.csv'
def run_etl_pipeline():
    pipeline = beam.Pipeline()

    # Read data from Cloud Storage
    input_data = pipeline | 'ReadData' >> beam.io.ReadFromText(input_file)

    # Write data to BigQuery
    input_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
        table='gcp-etl-project.gcp_etl_db.product',
        schema='field1:INTEGER, field2:STRING, field3:STRING, field4:INTEGER, field5:INTEGER',
        custom_gcs_temp_location='gs://gcp-etl-csv-files/temp-location/',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    pipeline.run().wait_until_finish()



if __name__ == '__main__':
    run_etl_pipeline()
