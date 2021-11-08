const Redis = require("ioredis");

const redisURIs = {
  'local': {"host": "localhost:6379"},
  'prod': {
    "host": [
      {
        "host": "srv-pr-ramdb1-dc3.prod.illiad.51b.tech",
        "port": 16379
      },
      {
        "host": "srv-pr-ramdb3-dc2.prod.illiad.51b.tech",
        "port": 16379
      },
      {
        "host": "srv-pr-ramdb4-dc2.prod.illiad.51b.tech",
        "port": 16379
      }
    ],
    "password": "yorHafdekjamHilyoofnucBifu",
    "name": "prod-fpc-sendmail",
  },
  'staging': {
    "host": [
      {
        "host": "sentinel-srv-inst-st-redis1.51b.tech",
        "port": 16379
      },
      {
        "host": "sentinel-srv-inst-st-redis2.51b.tech",
        "port": 16379
      },
      {
        "host": "sentinel-srv-inst-st-redis3.51b.tech",
        "port": 16379
      }
    ],
    "password": "KapemIjtap",
    "name": "st-redis-sendmail"
  },
}

const createRedisConnection = (env) => new Promise((resolve, reject) => {
  try {
    let redisInstance;
    if (env === 'local') {
      redisInstance = new Redis(redisURIs[env].host)
    } else {
      redisInstance = new Redis({
        sentinels: redisURIs[env].host,
        password: redisURIs[env].password,
        name: redisURIs[env].name,
      });
    }
    redisInstance.instanceName = "redisConnection"
    redisInstance.on('error', (err) => {
      if (err) {
        console.log(`[RDS_DEBUG] ${redisInstance.instanceName} err=${err}`)
      }
    })

    redisInstance.on('close', (err) => {
      if (err) {
        console.log(`[RDS_DEBUG] ${redisInstance.instanceName} closed err=${err}`)
      }
    })

    redisInstance.on('reconnecting', (err) => {
      if (err) {
        console.log(`[RDS_DEBUG] ${redisInstance.instanceName} reconnecting err=${err}`)
      }
    })

    redisInstance.on('end', (err) => {
      if (err) {
        console.log(`[RDS_DEBUG] ${redisInstance.instanceName} end err=${err}`)
      }
    })

    redisInstance.on('ready', () => resolve(redisInstance))
  } catch (e) {
    return reject(e)
  }
  return false
})

const getAllMetaList = async (redisConn) => {
  const keys = await redisConn.keys("*")
  return keys
}

const testFn = async () => {
  const env = process.env.NODE_ENV
  const redisConn = await createRedisConnection(env)
  const metaKeys = await getAllMetaList(redisConn)
  console.log(metaKeys);
}

testFn()