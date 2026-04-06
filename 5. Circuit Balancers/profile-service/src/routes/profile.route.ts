import { Router } from 'express'

const router = Router()

router.get('/profile/:id', (req, res) => {
  const { id } = req.params
  const name = `$Name{id}`
  const bio = `$Bio{id}`
  res.json({id,name,bio})
})





export default router
