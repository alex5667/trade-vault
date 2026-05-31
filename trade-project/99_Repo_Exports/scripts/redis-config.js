// 🔧 КОНФИГУРАЦИЯ REDIS ДЛЯ РАЗНЫХ СРЕД

const config = {
  development: {
    host: 'localhost',
    port: 6379,
    db: 0,
    // Настройки для разработки
    connectTimeout: 5000,
    commandTimeout: 3000,
    keepAlive: 30000,
    family: 4,
    maxRetriesPerRequest: 3,
    retryDelayOnFailover: 100,
    enableReadyCheck: false,
    lazyConnect: true,
    // Настройки пула
    poolSize: 5,
    maxConnections: 20
  },
  
  production: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    db: parseInt(process.env.REDIS_DB) || 0,
    password: process.env.REDIS_PASSWORD,
    // Настройки для продакшена
    connectTimeout: 10000,
    commandTimeout: 5000,
    keepAlive: 30000,
    family: 4,
    maxRetriesPerRequest: 3,
    retryDelayOnFailover: 100,
    enableReadyCheck: false,
    lazyConnect: true,
    // Настройки пула
    poolSize: 10,
    maxConnections: 50,
    // Дополнительные настройки для стабильности
    retryDelayOnClusterDown: 300,
    maxRetriesPerRequest: 3,
    // Настройки для кластера
    enableOfflineQueue: false,
    maxMemoryPolicy: 'allkeys-lru',
    maxMemory: '16gb'
  },
  
  test: {
    host: 'localhost',
    port: 6379,
    db: 1, // Отдельная БД для тестов
    // Настройки для тестов
    connectTimeout: 2000,
    commandTimeout: 1000,
    keepAlive: 10000,
    family: 4,
    maxRetriesPerRequest: 1,
    retryDelayOnFailover: 50,
    enableReadyCheck: false,
    lazyConnect: true,
    // Настройки пула
    poolSize: 2,
    maxConnections: 5
  }
};

// Функция для получения конфигурации
function getRedisConfig(environment = process.env.NODE_ENV || 'development') {
  const envConfig = config[environment];
  
  if (!envConfig) {
    throw new Error(`Unknown environment: ${environment}`);
  }
  
  // Добавляем переменные окружения если они есть
  return {
    ...envConfig,
    host: process.env.REDIS_HOST || envConfig.host,
    port: parseInt(process.env.REDIS_PORT) || envConfig.port,
    db: parseInt(process.env.REDIS_DB) || envConfig.db,
    password: process.env.REDIS_PASSWORD || envConfig.password,
  };
}

// Функция для создания Redis сервиса
function createRedisService(environment = process.env.NODE_ENV || 'development') {
  const { RedisService } = require('./redis-client-optimized');
  const config = getRedisConfig(environment);
  
  return new RedisService(config);
}

// Функция для создания Redis клиента
function createRedisClient(environment = process.env.NODE_ENV || 'development') {
  const { RedisClientFactory } = require('./redis-client-optimized');
  const config = getRedisConfig(environment);
  
  return RedisClientFactory.createClient(config);
}

// Функция для проверки подключения
async function checkRedisConnection(redisService) {
  try {
    await redisService.executeCommand('ping');
    console.log('✅ Redis connection successful');
    return true;
  } catch (error) {
    console.error('❌ Redis connection failed:', error.message);
    return false;
  }
}

// Функция для мониторинга Redis
async function monitorRedis(redisService) {
  try {
    const info = await redisService.executeCommand('info', 'server');
    const memory = await redisService.executeCommand('info', 'memory');
    const clients = await redisService.executeCommand('info', 'clients');
    
    return {
      server: parseRedisInfo(info),
      memory: parseRedisInfo(memory),
      clients: parseRedisInfo(clients),
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error('Redis monitoring error:', error);
    return null;
  }
}

// Парсер Redis INFO
function parseRedisInfo(infoString) {
  const lines = infoString.split('\r\n');
  const info = {};
  
  for (const line of lines) {
    if (line && !line.startsWith('#')) {
      const [key, value] = line.split(':');
      if (key && value) {
        info[key] = value;
      }
    }
  }
  
  return info;
}

// Graceful shutdown
async function gracefulShutdown(redisService) {
  console.log('🔄 Gracefully shutting down Redis connections...');
  
  try {
    await redisService.disconnect();
    console.log('✅ Redis connections closed successfully');
  } catch (error) {
    console.error('❌ Error closing Redis connections:', error);
  }
}

// Обработка сигналов для graceful shutdown
process.on('SIGINT', async () => {
  console.log('Received SIGINT, shutting down gracefully...');
  await gracefulShutdown(global.redisService);
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Received SIGTERM, shutting down gracefully...');
  await gracefulShutdown(global.redisService);
  process.exit(0);
});

module.exports = {
  getRedisConfig,
  createRedisService,
  createRedisClient,
  checkRedisConnection,
  monitorRedis,
  gracefulShutdown,
  config
};
