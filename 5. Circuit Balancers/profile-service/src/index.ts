import express from 'express'

const app = express()
const PORT = process.env.PORT || 3002

app.use(express.json())


app.listen(PORT, () => {
  console.log(`Profile service listening on port ${PORT}`)
})
