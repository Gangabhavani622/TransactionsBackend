const express = require("express");
const cors = require('cors');
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");
const path = require("path");
const fetch = require('cross-fetch');




const app = express();


app.use(express.json());
app.use(cors());

const dbPath = path.join(__dirname, "transcationsApplication.db");
let db = null;

const initializeDbAndServer = async () => {
  try {
    db = await open({
      filename: dbPath,
      driver: sqlite3.Database,
    });

    // Fetch data from the third-party API using fetch
    const apiUrl =
      "https://s3.amazonaws.com/roxiler.com/product_transaction.json";
    const response = await fetch(apiUrl);
    const data = await response.json();

    // Initialize database with seed data
    await initializeDatabase(data);

    app.listen(5000, () => {
      console.log("Server started on port 5000");
    });
  } catch (e) {
    console.error(`DB Error: ${e.message}`);
    process.exit(1);
  }
};

const initializeDatabase = async (data) => {
  // Create your table
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        title TEXT,
        price REAL,
        description TEXT,
        category TEXT,
        image TEXT,
        sold BOOLEAN,
        dateOfSale DATETIME
    )
  `;
  await db.run(createTableQuery);

  // Insert data into the table with a single query
  const insertDataQuery = `
  INSERT OR IGNORE INTO products (id, title, price, description, category, image, sold, dateOfSale)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`;

  for (const product of data) {
    await db.run(insertDataQuery, [
      product.id,
      product.title,
      product.price,
      product.description,
      product.category,
      product.image,
      product.sold ? 1 : 0,
      product.dateOfSale,
    ]);
  }
};

initializeDbAndServer();

//API-1
app.get("/transactions/", async (request, response) => {
  const { search_q = "", page = 1, per_page = 10, selectedMonth="03" } = request.query;
  const formattedMonth =
    selectedMonth.length === 1 ? `0${selectedMonth}` : selectedMonth;
  const getTransactionsQuery = `
      SELECT * FROM products
      WHERE(title LIKE '%${search_q}%' OR description LIKE '%${search_q}%' OR price LIKE '%${search_q}%') 
      AND strftime('%m', datetime(dateOfSale, 'localtime')) = '${formattedMonth}';
      LIMIT ${per_page} OFFSET ${(page - 1) * per_page};
    `;
  const transactionsResponse = await db.all(getTransactionsQuery);
  response.send(transactionsResponse);
});

//API-2
app.get("/statistics/", async (request, response) => {
  const { selectedMonth="03" } = request.query;
  const formattedMonth =
    selectedMonth.length === 1 ? `0${selectedMonth}` : selectedMonth;
  const getStatisticsQuery = `
    SELECT
      SUM(CASE WHEN sold THEN price ELSE 0 END) as totalSaleAmount,
      SUM(CASE WHEN sold THEN 1 ELSE 0 END) as totalSoldItems,
      SUM(CASE WHEN NOT sold THEN 1 ELSE 0 END) as totalNotSoldItems
    FROM products
    WHERE strftime('%m', datetime(dateOfSale, 'localtime')) = '${formattedMonth}';
  `;

  const statisticsResponse = await db.get(getStatisticsQuery);
  response.send(statisticsResponse);
});

//API-3
app.get("/bar-chart/", async (request, response) => {
  const { selectedMonth="03" } = request.query;
  const formattedMonth =
    selectedMonth.length === 1 ? `0${selectedMonth}` : selectedMonth;
  const getBarChartDataQuery = `
    SELECT
      CASE 
        WHEN price BETWEEN 0 AND 100 THEN '0 - 100'
        WHEN price BETWEEN 101 AND 200 THEN '101 - 200'
        WHEN price BETWEEN 201 AND 300 THEN '201 - 300'
        WHEN price BETWEEN 301 AND 400 THEN '301 - 400'
        WHEN price BETWEEN 401 AND 500 THEN '401 - 500'
        WHEN price BETWEEN 501 AND 600 THEN '501 - 600'
        WHEN price BETWEEN 601 AND 700 THEN '601 - 700'
        WHEN price BETWEEN 701 AND 800 THEN '701 - 800'
        WHEN price BETWEEN 801 AND 900 THEN '801 - 900'
        ELSE '901-above'
      END as priceRange,
      COUNT(*) as itemCount
    FROM products
    WHERE strftime('%m', datetime(dateOfSale, 'localtime')) = '${formattedMonth}'
    GROUP BY priceRange;
  `;

  const barChartData = await db.all(getBarChartDataQuery);
  response.send(barChartData);
});

//API-4
app.get("/pie-chart/:selectedMonth", async (request, response) => {
  const { selectedMonth } = request.params;
  const formattedMonth =
    selectedMonth.length === 1 ? `0${selectedMonth}` : selectedMonth;

  const getPieChartDataQuery = `
    SELECT
      category,
      COUNT(*) as itemCount
    FROM products
    WHERE strftime('%m', date(dateOfSale, 'localtime')) = '${formattedMonth}'
    GROUP BY category;
  `;

  const pieChartData = await db.all(getPieChartDataQuery);
  response.send(pieChartData);
});


module.exports=app;