"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createAllPartitions = exports.batchCreatePartition = void 0;

var _awsSdk = _interopRequireDefault(require("aws-sdk"));

var _store = require("./store");

var _columns = _interopRequireDefault(require("./columns"));

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

_awsSdk.default.config.update({
  region: process.env.AWS_DEFAULT_REGION
});
/**
 * batchCreatePartition
 *
 * A batch of no more than 100 partitions to create as a batch in AWS Glue
 * @param {Object} glue - AWS Glue service object
 * @param {Object} partitions - Array of up to 100 partitions
 */


const batchCreatePartition = (glue, partitions, bucket, path) => new Promise((resolve, reject) => {
  console.log(`Attempting to create ${partitions.length} partitions`);
  glue.batchCreatePartition({
    DatabaseName: 'default',
    TableName: 'cloudtrail_logs',
    PartitionInputList: partitions.map(({
      account,
      region,
      year,
      month,
      day
    }) => ({
      Values: [account, region, year, month, day],
      StorageDescriptor: {
        Columns: _columns.default,
        Location: `s3://${bucket}/${path}${account}/CloudTrail/${region}/${year}/${month}/${day}/`,
        InputFormat: 'com.amazon.emr.cloudtrail.CloudTrailInputFormat',
        OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        SerdeInfo: {
          SerializationLibrary: 'com.amazon.emr.hive.serde.CloudTrailSerde',
          Parameters: {
            'serialization.format': '1'
          }
        }
      }
    }))
  }, (err, data) => {
    if (err) {
      reject(err);
    } else {
      resolve(data);
    }
  });
});
/**
 * createAllPartitions
 *
 * creates all listed partitions in separate batch requests
*/


exports.batchCreatePartition = batchCreatePartition;

const createAllPartitions = async (partitions, bucket, path) => {
  const glue = new _awsSdk.default.Glue({
    apiVersion: '2017-03-31'
  });

  while (partitions.length > 0) {
    const batch = partitions.splice(0, 100);
    await batchCreatePartition(glue, batch, bucket, path);
    await Promise.all(batch.map(({
      account,
      region,
      year,
      month,
      day
    }) => (0, _store.savePartitionRecordForPath)(`${account}/${region}/${year}/${month}/${day}`)));
  }
};

exports.createAllPartitions = createAllPartitions;