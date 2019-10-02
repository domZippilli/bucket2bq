#!/usr/bin/env bash

# Copyright 2019 Google Inc. All Rights Reserved.
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

# Display script usage
function usage(){
    echo >&2
    echo "Usage: $0 bucket_name metadata_obj dataset_table [temp_dataset_table]" >&2
    echo >&2
    echo "Loads a record of metadata for all objects in the given bucket. The metadata fields recorded are:" >&2
    echo >&2
    echo "  url: The URL of the object." >&2
    echo "  created: The creation time of the object." >&2
    echo "  updated: The last update time of the object." >&2
    echo "  storage_class: (MULTI_)REGIONAL/NEARLINE/COLDLINE storage class for the object." >&2
    echo "  size: The content-length of the object." >&2
    echo "  content_type: The content type of the object, if one was supplied at upload." >&2
    echo "  crc32: The stored crc32 hash of the object." >&2
    echo "  md5: The stored MD5 hash of the object." >&2
    echo >&2
    echo "Arguments:" >&2
    echo "  bucket_name             The name of the bucket from which to query metadata. Example: gs://mybucket" >&2
    echo "  metadata_obj            The name of an object in which to stream LDJSON metadata. This object can be deleted when the job is done. Example: gs://mybucket/metadata.json" >&2
    echo "  dataset_table           The name of the dataset and table in which to store the metadata in BigQuery, separated by a dot. Example: mydataset.mytable" >&2
    echo "  temp_dataset_table      (optional) The name of a temporary dataset and table in which to temporarily store the metadata in BigQuery, separated by a dot. A temporary table is used for timestamp parsing. Example: mydataset.mytable_temp" >&2
    echo >&2
}

BUCKET_NAME=${1?$(usage)}
METADATA_JSON=${2?$(usage)}
DATASET_TABLE=${3?$(usage)}
TEMP_DATASET_TABLE=${4:-$3_temp}

# Get the recursive listing and send parsed JSON to stdout.
function get_objects {
    echo Getting bucket list... >&2
    # begin the pipeline with a recursive bucket listing
    gsutil ls -L $BUCKET_NAME/** |
    # remove extraneous lines
    grep -v "TOTAL|^$" |
    # filter to specific fields we want to parse and include
    grep "gs://\|Creation time\|Update time\|Storage class\|Content-Length\|Content-Type\|Hash (" |
    # add an empty line to act as a record separator
    sed 's \gs:// \n\gs:// g' |
    # form JSON line with awk
    awk ' {RS=""} {ORS="\n"} {FS=": +|:\n|\n"} \
            {print \
                "{ \"url\": \"" $1 "\"," \
                "  \"created\": \"" $3 "\"," \
                "  \"updated\": \"" $5 "\"," \
                "  \"storage_class\": \"" $7 "\"," \
                "  \"size\": \"" $9 "\"," \
                "  \"content_type\": \"" $11 "\"," \
                "  \"crc32\": \"" $13 "\"," \
                "  \"md5\": \"" $15 "\" }" \
            }' |
    # remove the first line, which is an empty record
    tail -n+2 -f 
    echo Bucket listing complete. >&2
}

echo Streaming object metadata JSON to GCS...
    get_objects | tee /dev/tty | gsutil cp - $METADATA_JSON
echo Streaming JSON to single object complete...

echo Loading JSON object into $TEMP_DATASET_TABLE...
    bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --replace $TEMP_DATASET_TABLE $METADATA_JSON
echo Load complete to $TEMP_DATASET_TABLE. Querying with timestamp parsing to $DATASET_TABLE
    bq query -n 0 --destination_table $DATASET_TABLE --replace --use_legacy_sql=false \
    "SELECT 
    url,
    PARSE_TIMESTAMP('%a, %d %b %Y %T GMT', created, 'UTC') as created,
    PARSE_TIMESTAMP('%a, %d %b %Y %T GMT', updated, 'UTC') as updated,
    storage_class, size, content_type, crc32, md5
    FROM $TEMP_DATASET_TABLE"
    bq rm -f -t $TEMP_DATASET_TABLE
echo Load complete. Happy querying!
