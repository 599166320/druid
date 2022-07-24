SELECT
SUMMARY_EXACT(v,'max,min,mean,p0.95'),b,c,__time
FROM quantile_test01
GROUP BY b,c,__time

{
"type": "index_parallel",
"id": "index_parallel_quantile_test01_oldafcfn_2022-07-23T13:00:09.297Z",
"groupId": "index_parallel_quantile_test01_oldafcfn_2022-07-23T13:00:09.297Z",
"resource": {
"availabilityGroup": "index_parallel_quantile_test01_oldafcfn_2022-07-23T13:00:09.297Z",
"requiredCapacity": 1
},
"spec": {
"dataSchema": {
"dataSource": "quantile_test01",
"timestampSpec": {
"column": "a",
"format": "yyyyMMddHHmmss",
"missingValue": null
},
"dimensionsSpec": {
"dimensions": [
{
"type": "string",
"name": "b",
"multiValueHandling": "SORTED_ARRAY",
"createBitmapIndex": true
},
{
"type": "string",
"name": "c",
"multiValueHandling": "SORTED_ARRAY",
"createBitmapIndex": true
}
],
"dimensionExclusions": [
"__time",
"a",
"d",
"v"
]
},
"metricsSpec": [
{
"type": "valueAppend",
"name": "v",
"fieldName": "d",
"maxIntermediateSize": 0,
"fun": null
}
],
"granularitySpec": {
"type": "uniform",
"segmentGranularity": "HOUR",
"queryGranularity": "HOUR",
"rollup": true,
"intervals": []
},
"transformSpec": {
"filter": null,
"transforms": []
}
},
"ioConfig": {
"type": "index_parallel",
"inputSource": {
"type": "inline",
"data": "20220425000000\ta\t3\t1000\n20220425000100\ta\t3\t1001\n20220425000200\tc\t3\t1003\n20220425000300\tc\t3\t1004\n20220425000400\td\t3\t1004\n20220425000500\td\t3\t1005\n20220425000600\td\t3\t1006\n20220425000700\te\t3\t1007\n20220425000800\ta\t3\t1008\n20220425000000\ta\t4\t1018\n20220425000100\ta\t4\t1028\n20220425000500\td\t4\t1038\n20220425000500\td\t4\t1038\n20220425000500\td\t4\t1038\n20220425000500\td\t4\t1038\n20220425000500\td\t4\t1038"
},
"inputFormat": {
"type": "tsv",
"columns": [
"a",
"b",
"c",
"d"
],
"listDelimiter": null,
"delimiter": "\t",
"findColumnsFromHeader": false,
"skipHeaderRows": 0
},
"appendToExisting": false,
"dropExisting": false
},
"tuningConfig": {
"type": "index_parallel",
"maxRowsPerSegment": 5000000,
"appendableIndexSpec": {
"type": "onheap"
},
"maxRowsInMemory": 1000000,
"maxBytesInMemory": 0,
"skipBytesInMemoryOverheadCheck": false,
"maxTotalRows": null,
"numShards": null,
"splitHintSpec": null,
"partitionsSpec": {
"type": "hashed",
"numShards": null,
"partitionDimensions": [],
"partitionFunction": "murmur3_32_abs",
"maxRowsPerSegment": 5000000
},
"indexSpec": {
"bitmap": {
"type": "roaring",
"compressRunOnSerialization": true
},
"dimensionCompression": "lz4",
"metricCompression": "lz4",
"longEncoding": "longs",
"segmentLoader": null
},
"indexSpecForIntermediatePersists": {
"bitmap": {
"type": "roaring",
"compressRunOnSerialization": true
},
"dimensionCompression": "lz4",
"metricCompression": "lz4",
"longEncoding": "longs",
"segmentLoader": null
},
"maxPendingPersists": 0,
"forceGuaranteedRollup": true,
"reportParseExceptions": false,
"pushTimeout": 0,
"segmentWriteOutMediumFactory": null,
"maxNumConcurrentSubTasks": 1,
"maxRetry": 3,
"taskStatusCheckPeriodMs": 1000,
"chatHandlerTimeout": "PT10S",
"chatHandlerNumRetries": 5,
"maxNumSegmentsToMerge": 100,
"totalNumMergeTasks": 10,
"logParseExceptions": false,
"maxParseExceptions": 2147483647,
"maxSavedParseExceptions": 0,
"maxColumnsToMerge": -1,
"awaitSegmentAvailabilityTimeoutMillis": 0,
"partitionDimensions": []
}
},
"context": {
"forceTimeChunkLock": true,
"useLineageBasedSegmentAllocation": true
},
"dataSource": "quantile_test01"
}