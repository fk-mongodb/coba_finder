/**
 * @fileoverview Query 2-hr weather forecast by area and date range
 *
 * @description
 * Query 2-hr weather forecast by area and date range
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
  const toTs = "2025-03-01T06:00:00.000Z";

  const fromMoment = moment(fromTs);
  const toMoment = moment(toTs);

  const matchByAreaStage = {
    $match: {
      area: { $regex: `.*${areaKeywordToSearch}.*`, $options: "i" },
    },
  };

  const setTsStage = {
    $set: {
      start: {
        $dateFromString: {
          dateString: "$period.start",
        },
      },
      end: {
        $dateFromString: {
          dateString: "$period.end",
        },
      },
      ts: {
        $dateFromString: {
          dateString: "$timestamp",
        },
      },
    },
  };

  const projectFieldsStage = {
    $project: {
      _id: -1,
      area: 1,
      start: 1,
      end: 1,
      ts: 1,
      forecast: 1,
    },
  };

  const matchByTsDtRangeStage = {
    $match: {
      $and: [
        {
          ts: { $gte: fromMoment.toDate() },
        },
        {
          ts: { $lte: toMoment.toDate() },
        },
      ],
    },
  };

  const densifyStage = {
    $densify: {
      field: "ts",
      range: {
        step: 6,
        unit: "hour",
        bounds: [fromMoment.toDate(), toMoment.toDate()],
      },
    },
  };

  const fillStage = {
    $fill: {
      output: {
        area: { value: "NA" },
        forecast: { value: "NA" },
      },
    },
  };

  const sortStage = {
    $sort: {
      ts: -1,
      area: 1,
    },
  };

  const limitStage = {
    $limit: 1000,
  };

  const pipeline = [
    matchByAreaStage,
    setTsStage,
    projectFieldsStage,
    matchByTsDtRangeStage,
    densifyStage,
    fillStage,
    sortStage,
    limitStage,
  ];

  const forecastColl = db.collection("two_hr_forecast_by_area");

  const customPromise = new Promise(async (resolve, reject) => {
    const res = await forecastColl.aggregate(pipeline).toArray();
    console.log(res);
    resolve();
  });

  return customPromise;
}

main().catch(console.error);
