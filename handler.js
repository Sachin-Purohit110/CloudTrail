"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.handler = void 0;

var _s = require("./s3");

var _store = require("./store");

var _partition = require("./partition");

/**
* Parameters (env vars)
*
* 1) BUCKET_NAME - name of the bucket containing the CloudTrail logs
* 2) ORG_ID - the id of the AWS Organization to consider when creating/partitioning the database
* 3) DATABASE - the name of the Athena database in which you want to create the table
* 4) TABLE_NAME - the name of the table to create in the Athena database in DATABASE
*/
const handler = async () => {
  // console.log('Received event:', JSON.stringify(event, null, 2));
  const {
    BUCKET_NAME: bucket,
    ORGANIZATION_ID: orgId
  } = process.env;
  const path = orgId ? `AWSLogs/${orgId}/` : 'AWSLogs/';
  const partitionTree = await (0, _s.getAllParitions)(bucket, path);
  console.log(JSON.stringify(partitionTree));
  const partitions = await (0, _store.constructNewPartitionKeySetsFromTree)(partitionTree);
  console.log('Partitions');
  console.log(JSON.stringify(partitions));
  await (0, _partition.createAllPartitions)(partitions, bucket, path);
};

exports.handler = handler;
var _default = handler;
exports.default = _default;