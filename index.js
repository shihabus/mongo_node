const { MongoClient } = require("mongodb");
require("dotenv").config();

async function main() {
  /**
   * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
   * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
   */
  const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@postman.dyh9i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority`;

  /**
   * The Mongo Client you will use to interact with your database
   * See https://mongodb.github.io/node-mongodb-native/3.6/api/MongoClient.html for more details
   */
  const client = new MongoClient(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });

  try {
    // Connect to the MongoDB cluster
    await client.connect();

    // Make the appropriate DB calls
    // list a db
    // await listDatabases(client);

    // insert a single document/row
    // await createListing(client, {
    //   name: "Lovely loft",
    //   summary: "A charming loft in Paris",
    //   bedrooms: 1,
    //   bathrooms: 1,
    // });

    // --- insert multiple docs ------
    // await multipleListings(client, [
    //   {
    //     name: "Narayana",
    //     summary: "A charming loft in Paris",
    //     bedrooms: 2,
    //     bathrooms: 1,
    //   },
    //   {
    //     name: "Atrium",
    //     property_name: "Atrium Coorg",
    //     summary: "With pool",
    //     bedrooms: 5,
    //     bathrooms: 3,
    //   },
    // ]);

    // --- findOne ----
    // await findOneListingByName(client, "Narayana");

    // --- find many using find ---
    // await findListingsWithMinimumBedAndBathWithRecentReviews(client, {
    //   minBeds: 4,
    //   minBaths: 2,
    //   maxResults: 5,
    // });

    // --- updateOne ---
    // await findOneListingByName(client, "Narayana");
    // await updateListingByName(client, "Narayana", {
    //   bedrooms: 6,
    //   bathrooms: 4,
    // });
    // await findOneListingByName(client, "Narayana");

    // ---upsert ---
    // await findOneListingByName(client, "Taj");
    // await upsertListingByName(client, "Taj", {
    //   bedrooms: 2,
    //   bathrooms: 1,
    // });
    // await findOneListingByName(client, "Taj");

    // -- updateMany --
    // await updateAllListingToHaveProperty(client);

    // -- deleteOne --
    // await deleteListingByName(client, "Taj");

    // -- deleteMany --
    await deleteListingScrapedBeforeDate(client, new Date("2019-02-15"));
  } catch (e) {
    console.error(e);
  } finally {
    // Close the connection to the MongoDB cluster
    await client.close();
  }
}

main().catch(console.error);

async function deleteListingScrapedBeforeDate(client, date) {
  const results = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .deleteMany({ last_scraped: { $lt: date } });
  console.log(`${results.deletedCount} docs deleted`);
}

async function deleteListingByName(client, nameOfListing) {
  const result = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .deleteOne({ name: nameOfListing });

  console.log(`${result.deletedCount} document(s) was/were deleted`);
}

async function updateAllListingToHaveProperty(client) {
  const result = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .updateMany(
      {
        property_name: { $exists: false },
      },
      { $set: { property_name: "Unknown" } }
    );
  console.log(`${result.matchedCount} documents matching the query`);
  console.log(`${result.modifiedCount} documents are updated`);
}

async function upsertListingByName(client, nameOfListing, updatedListing) {
  const result = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .updateOne(
      {
        name: nameOfListing,
      },
      {
        // shallow merge
        $set: updatedListing,
      },
      { upsert: true }
    );

  console.log(`${result.matchedCount} documents matching the query`);
  if (result.upsertedCount > 0) {
    console.log(`${result.upsertedId._id} is upsert`);
  } else {
    console.log(`${result.modifiedCount} documents are updated`);
  }
}

async function updateListingByName(client, nameOfListing, updatedListing) {
  const result = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .updateOne(
      {
        name: nameOfListing,
      },
      {
        // shallow merge
        $set: updatedListing,
      }
    );

  console.log(`${result.matchedCount} documents matching the query`);
  console.log(`${result.modifiedCount} documents are updated`);
}

async function findListingsWithMinimumBedAndBathWithRecentReviews(
  client,
  { minBeds = 0, minBaths = 0, maxResults = Number.MAX_SAFE_INTEGER }
) {
  const cursor = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .find({
      bedrooms: {
        $gte: minBeds,
      },
      bathrooms: {
        $gte: minBaths,
      },
    })
    // most recent review
    .sort({ last_review: -1 })
    .limit(maxResults);

  const results = await cursor.toArray();
  if (results.length > 0) {
    console.log("results", results);
  } else {
    console.log("No results. Oops");
  }
}

async function findOneListingByName(client, nameOfListing) {
  const results = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .findOne({ name: nameOfListing });
  if (results) {
    console.log("results", results);
  } else {
    console.log("No results. Oops");
  }
}

async function multipleListings(client, newListings) {
  const results = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .insertMany(newListings);
  console.log(`${results.insertedCount} listing created with`);
  console.log(results.insertedIds);
}

async function createListing(client, newList) {
  const result = await client
    .db("sample_airbnb")
    .collection("listingsAndReviews")
    .insertOne(newList);
  console.log("New listing is created with id", result.insertedId);
}

/**
 * Print the names of all available databases
 * @param {MongoClient} client A MongoClient that is connected to a cluster
 */
async function listDatabases(client) {
  databasesList = await client.db().admin().listDatabases();

  console.log("Databases:");
  databasesList.databases.forEach((db) => console.log(` - ${db.name}`));
}
