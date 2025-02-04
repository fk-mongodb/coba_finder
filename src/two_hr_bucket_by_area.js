/**
 * @fileoverview Query 2-hr weather forecast and place into bucket of area
 *
 * @description
 *  Query 2-hr weather forecast and place into bucket of area
 *
 * @author Fernando Karnagi <fkarnagi@gmail.com>
 * @version 1.0.0
 * @date 31-Jan-2025
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

  const areaKeywordToSearch = "";
  const fromTs = "2025-01-26T06:00:00.000Z";
  const toTs = "2025-02-01T06:00:00.000Z";

  const fromMoment = moment(fromTs);
  const toMoment = moment(toTs);

  const setTsStage = {
    $set: {
      ts: {
        $dateFromString: {
          dateString: "$timestamp",
        },
      },
      initial: {
        $substr: ["$area", 0, 1],
      },
    },
  };

  const bucketStage = {
    $bucket: {
      groupBy: "$initial", // Field to group by
      boundaries: ["A", "B", "C", "D"], // Boundaries for the buckets
      default: "Others", // Bucket ID for documents which do not fall into a bucket
      output: {
        // Output for each bucket
        count: { $sum: 1 },
        areas: {
          $push: {
            area: "$area",
            forecast: "$forecast",
            timestamp: "$timestamp",
          },
        },
        periods: {
          $push: {
            area: "$area",
            period: "$period",
            timestamp: "$timestamp",
          },
        },
      },
    },
  };

  const setPostStage = {
    $set: {
      initial: "$_id",
    },
  };

  const unsetPostStage = {
    $unset: ["_id"],
  };

  const sortStage = {
    $sort: {
      initial: 1,
      // ts: -1,
    },
  };

  const limitStage = {
    $limit: 10,
  };

  const pipeline = [
    setTsStage,
    bucketStage,
    setPostStage,
    sortStage,
    unsetPostStage,
    limitStage,
  ];

  const forecastColl = db.collection("two_hr_forecast_by_area");

  const customPromise = new Promise(async (resolve, reject) => {
    const res = await forecastColl.aggregate(pipeline).toArray();
    console.log(res[0]);
    resolve();
  });

  return customPromise;
}

main().catch(console.error);
