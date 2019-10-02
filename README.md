# bucket2bq
A script that loads a detailed GCS bucket listing into BigQuery, so you can analyze object metadata.

```Usage: ./bucket2bq.sh bucket_name metadata_obj dataset_table [temp_dataset_table]

Loads a record of metadata for all objects in the given bucket. The metadata fields recorded are:

  url: The URL of the object.
  created: The creation time of the object.
  updated: The last update time of the object.
  storage_class: (MULTI_)REGIONAL/NEARLINE/COLDLINE storage class for the object.
  size: The content-length of the object.
  content_type: The content type of the object, if one was supplied at upload.
  crc32: The stored crc32 hash of the object.
  md5: The stored MD5 hash of the object.

Arguments:
  bucket_name             The name of the bucket from which to query metadata. Example: gs://mybucket
  metadata_obj            The name of an object in which to stream LDJSON metadata. This object can be deleted when the job is done. Example: gs://mybucket/metadata.json
  dataset_table           The name of the dataset and table in which to store the metadata in BigQuery, separated by a dot. Example: mydataset.mytable
  temp_dataset_table      (optional) The name of a temporary dataset and table in which to temporarily store the metadata in BigQuery, separated by a dot. A temporary table is used for timestamp parsing. Example: mydataset.mytable_temp
```
