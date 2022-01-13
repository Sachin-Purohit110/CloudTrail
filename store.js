"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.constructNewPartitionKeySetsFromTree = exports.constructPartitionsForNewAccountOrCheckRegions = exports.constructPartitionsForNewRegionOrCheckYears = exports.constructPartitionsForNewYearOrCheckMonths = exports.constructPartitionsForNewMonthOrCheckDays = exports.savePartitionRecordForPath = exports.hasSomePartitionsForPath = exports.createPartitionDomainKey = void 0;

var _awsSdk = _interopRequireDefault(require("aws-sdk"));

var _lodash = require("lodash");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

_awsSdk.default.config.update({
  region: process.env.AWS_DEFAULT_REGION
});
/* Persistence */


const createPartitionDomainKey = (account, region, year, month, day) => `${account}/${region}/${year}/${month}/${day}`;

exports.createPartitionDomainKey = createPartitionDomainKey;

const hasSomePartitionsForPath = partitionDomainKey => new Promise((resolve, reject) => {
  const dynamodb = new _awsSdk.default.DynamoDB();
  const params = {
    Key: {
      Domain: {
        S: partitionDomainKey
      }
    },
    TableName: 'Athena-CloudTrail-Partitions'
  };
  dynamodb.getItem(params, (err, data) => {
    if (err) {
      reject(err);
    } else {
      resolve(typeof data.Item !== 'undefined');
    }
  });
});

exports.hasSomePartitionsForPath = hasSomePartitionsForPath;

const savePartitionRecordForPath = partitionDomainKey => new Promise((resolve, reject) => {
  const dynamodb = new _awsSdk.default.DynamoDB();
  const params = {
    Item: {
      Domain: {
        S: partitionDomainKey
      },
      Date: {
        S: new Date().toISOString()
      }
    },
    TableName: 'Athena-CloudTrail-Partitions'
  };
  dynamodb.putItem(params, (err, data) => {
    if (err) {
      reject(err);
    } else {
      resolve(data);
    }
  });
});
/* Checks for each level of the tree */


exports.savePartitionRecordForPath = savePartitionRecordForPath;

const constructPartitionsForNewMonthOrCheckDays = async (account, region, year, month, days) => {
  // otherwise, check for each day
  const partitions = await Promise.all(days.map(async day => {
    const recordFoundForDay = await hasSomePartitionsForPath(`${account}/${region}/${year}/${month}/${day}`);
    return recordFoundForDay ? null : {
      account,
      region,
      year,
      month,
      day
    };
  }));
  return partitions.filter(partition => partition !== null);
};

exports.constructPartitionsForNewMonthOrCheckDays = constructPartitionsForNewMonthOrCheckDays;

const constructPartitionsForNewYearOrCheckMonths = async (account, region, year, months) => {
  // otherwise, check for each month
  const partitions = await Promise.all(months.map(({
    name: month,
    children: days
  }) => constructPartitionsForNewMonthOrCheckDays(account, region, year, month, days)));
  return (0, _lodash.flatten)(partitions);
};

exports.constructPartitionsForNewYearOrCheckMonths = constructPartitionsForNewYearOrCheckMonths;

const constructPartitionsForNewRegionOrCheckYears = async (account, region, years) => {
  // otherwise, check for each year
  const partitions = await Promise.all(years.map(({
    name: year,
    children: months
  }) => constructPartitionsForNewYearOrCheckMonths(account, region, year, months)));
  return (0, _lodash.flatten)(partitions);
};

exports.constructPartitionsForNewRegionOrCheckYears = constructPartitionsForNewRegionOrCheckYears;

const constructPartitionsForNewAccountOrCheckRegions = async (account, regions) => {
  // otherwise, check for each region
  const partitions = await Promise.all(regions.map(({
    name: region,
    children: years
  }) => constructPartitionsForNewRegionOrCheckYears(account, region, years)));
  return (0, _lodash.flatten)(partitions);
};
/**
 * accepts a partition tree and checks each tier of the key hierarchy to determine
 * which subsets of available paritions require creation
 *
 * @param {Array} partitionTree
 */


exports.constructPartitionsForNewAccountOrCheckRegions = constructPartitionsForNewAccountOrCheckRegions;

const constructNewPartitionKeySetsFromTree = async partitionTree => {
  const partitions = await Promise.all(partitionTree.map(({
    name: account,
    children: regions
  }) => constructPartitionsForNewAccountOrCheckRegions(account, regions)));
  return (0, _lodash.flatten)((0, _lodash.flatten)(partitions));
};

exports.constructNewPartitionKeySetsFromTree = constructNewPartitionKeySetsFromTree;