import express from "express";
import { redis } from "./redis";
import { pg } from "./db";

const app = express();
app.use(express.json());

app.get('/ping', async (req, res) => {
  const result = await pg.query('SELECT NOW()')
  await redis.set('test', 'hello')
  const test = await redis.get('test')
  res.json({ pong: result.rows[0], test });
});

app.get('/users', async (req, res) => {

  try {
    const cacheKey = 'users:all'
    const cached = await redis.get(cacheKey)

    if (cached) {
      console.log('Cache hit')
      return res.json(JSON.parse(cached))
    }

    console.log('Cache miss')
    await new Promise(r => setTimeout(r, 2000))
    const result = await pg.query('SELECT * FROM users')
    await redis.set(cacheKey, JSON.stringify(result.rows), "EX", 30)
    res.json(result.rows)
  } catch (err) {
    console.error(err);
        res.status(500).json({ error: "Something went wrong" });
  }

});

app.get('/users/:id', async (req, res) => {
  const cacheKey = `users:${req.params.id}` //here in redis the data is stored as a key-value pair, if you wanna get all values then query as users:all if you wanna get a single value then query as users:id
  const cached = await redis.get(cacheKey)

  if (cached) {
    console.log('Cache hit')
    return res.json(JSON.parse(cached))
  }

  console.log('Cache miss')
  await new Promise(r => setTimeout(r, 2000))
  const result = await pg.query('SELECT * FROM users WHERE id = $1', [req.params.id])
  await redis.set(cacheKey, JSON.stringify(result.rows[0]),"EX",30)
  res.json(result.rows[0])
})


app.listen(3000, () => {
  console.log('Server is running on port 3000');
});
