import { Router } from 'express'


let isServiceHealthy = true


const router = Router()

router.get('/profile/:id', (req, res) => {
  if (isServiceHealthy) {
    const { id } = req.params
    const name = `$Name{id}`
    const bio = `$Bio{id}`
    res.json({id,name,bio})
  } else {
    res.status(500).json({ error: 'Service is unhealthy' })
  }
})

router.post('/toggle', (req, res) => {
  isServiceHealthy = !isServiceHealthy
  res.json({ healthy: isServiceHealthy })
})




export default router
