const Redis = require("ioredis");
const redisURIs = require("./redis_config.json")

let incorr = []
let corr = []
let crit = []

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

const getKeys = async (redisConn, pattern) => {
  const keys = await redisConn.keys(pattern)
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
  const cons = parseInt(allParts.active_consumers)
  if (cons < metaOtherKeyLen) {
    crit.push({
      key: metaKey,
      ac: allParts.active_consumers,
      ec: allParts.expected_consumers,
      km: metaOtherKeyLen
    })
  } else if (cons !== metaOtherKeyLen) {
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
  const metaKeys = await getKeys(redisConn, "metadata_*")

  if (metaKeys.length) {
    await Promise.all(
      metaKeys.map(async mk => {
        await getMetaRelevantInfo(redisConn, mk)
      })
    ).then(() => {
      console.log("CRITICAL TO CHECK --------> ( " , crit.length, " )");
      crit.forEach((data) => {
        console.log(`${data.key} : AC: ${data.ac}, EC: ${data.ec}, KEYMAP: ${data.km}`);
      })
      console.log("\n\n");

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

const findStuckMessages = async () => {
  const env = process.env.NODE_ENV
  const redisConn = await createRedisConnection(env)
  const clientKeys = await getKeys(redisConn, "*:*[0-9]")

  if (clientKeys.length) {
    await Promise.all(
      clientKeys.map(async clientQueue => {
        const len = await redisConn.llen(clientQueue)
        return len
      })
    ).then((result) => {
      let msgCount = 0
      result.forEach(r => {
        msgCount += r
      })
      console.log("Pending messages are: ", msgCount);
    })
  }
}

const processDef = () => {
  const p = process.argv[2]
  switch (p) {
    case "pending-msg": 
      findStuckMessages()
      return

    default:
      testFn()
  }
}

processDef()
