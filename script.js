const Redis = require("ioredis");
const redisURIs = require("./redis_config.json")

let incorr = []
let corr = []

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
  const keys = await redisConn.keys("metadata_*")
  return keys
}

const getMetaRelevantInfo = async (redisConn, metaKey) => {
  const allParts = await redisConn.hgetall(metaKey)
  let metaOtherKeyLen = Object.keys(allParts).length
  if (allParts.active_consumers) {
    metaOtherKeyLen -= 1
  }
  if (allParts.expected_consumers) {
    metaOtherKeyLen -= 1
  }
  if (parseInt(allParts.active_consumers) !== metaOtherKeyLen) {
    incorr.push({
      key: metaKey,
      ac: allParts.active_consumers,
      ec: allParts.expected_consumers,
      km: metaOtherKeyLen
    })
    
  } else {
    corr.push({
      key: metaKey,
      ac: allParts.active_consumers,
      ec: allParts.expected_consumers,
      km: metaOtherKeyLen
    })
  }
}

const testFn = async () => {
  const env = process.env.NODE_ENV
  const redisConn = await createRedisConnection(env)
  const metaKeys = await getAllMetaList(redisConn)

  if (metaKeys.length) {
    await Promise.all(
      metaKeys.map(async mk => {
        await getMetaRelevantInfo(redisConn, mk)
      })
    ).then(() => {
      console.log("INCORRECT / TO CHECK --------> ( " , incorr.length, " )");
      incorr.forEach((data) => {
        console.log(`${data.key} : AC: ${data.ac}, EC: ${data.ec}, KEYMAP: ${data.km}`);
      })
      console.log("\n\n");

      console.log("CORRECT --------> ( ", corr.length, " )");
      corr.forEach((data) => {
        console.log(`${data.key} : AC: ${data.ac}, EC: ${data.ec}, KEYMAP: ${data.km}`);
      })
      console.log("\n\n");
    })
  }
  return
}

testFn()