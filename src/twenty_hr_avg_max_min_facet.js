/**
 * @fileoverview Query 2-hr weather average, min, and max
 *
 * @description
 *  Query 2-hr weather average, min, and max
 *
 * @author Fernando Karnagi <fkarnagi@gmail.com>
 * @version 1.0.0
 * @date 2-Feb-2025
 *
 */

const { MongoClient } = require("mongodb");
const moment = require("moment");
const params = require("./const");
const axios = require("axios");

require("dotenv").config();

async function main() {
  /**
   * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
   * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
   */
  const uri = `mongodb+srv://${process.env.MONGODB_USER}:${process.env.MONGODB_PASSWORD}@${params.HOST}/?retryWrites=true&appName=${params.CLUSTER}`;

  const client = new MongoClient(uri);

  try {
    console.log(
      "Connecting to MongoDB Cluster ",
      params.HOST,
      " -> ",
      params.CLUSTER
    );
    await client.connect();

    console.log("Querying records");

    await ops(client);
  } catch (e) {
    console.error(e);
  } finally {
    await client.close();
  }
}

async function ops(client) {
  const db = await client.db("coba");

  const fromTs = "2025-01-26T06:00:00.000Z";
  const toTs = "2025-03-01T06:00:00.000Z";

  const fromMoment = moment(fromTs);
  const toMoment = moment(toTs);

  const dtTsConverter = {
    $dateFromString: {
      dateString: "$updatedTimestamp",
    },
  };

  const setTsStage = {
    $set: {
      ts: dtTsConverter,
    },
  };

  const matchByTsDtRangeStage = {
    $match: {
      $and: [
        {
          // gte operator in expression
          $expr: { $gte: ["$ts", fromMoment.toDate()] },
        },
        {
          // simple lte operator
          ts: { $lte: toMoment.toDate() },
        },
      ],
    },
  };

  const avgPipeline = [
    {
      $group: {
        _id: {
          $dateToString: { format: "%Y-%m", date: dtTsConverter },
        },
        avg: { $avg: "$temperature.low" },
      },
    },
  ];

  const minPipeline = [
    {
      $group: {
        _id: {
          $dateToString: { format: "%Y-%m", date: dtTsConverter },
        },
        min: { $min: "$temperature.low" },
      },
    },
  ];

  const maxPipeline = [
    {
      $group: {
        _id: {
          $dateToString: { format: "%Y-%m", date: dtTsConverter },
        },
        max: { $max: "$temperature.low" },
      },
    },
  ];

  const pipeline = [
    setTsStage,
    matchByTsDtRangeStage,
    {
      $facet: {
        avg: avgPipeline,
        min: minPipeline,
        max: maxPipeline,
      },
    },
  ];

  const forecastColl = db.collection("twenty_hr_forecast_general");

  const customPromise = new Promise(async (resolve, reject) => {
    const res = await forecastColl.aggregate(pipeline).toArray();
    console.log(res[0]);
    resolve();
  });

  return customPromise;
}

main().catch(console.error);
