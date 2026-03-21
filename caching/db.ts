import pkg from "pg"
const { Pool } = pkg
export const pg = new Pool({
  user: "vedant",
  host: "localhost",
  database: "ddia",
  password: "vedant",
  port: 5432,
})
