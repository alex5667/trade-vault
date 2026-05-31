// 🔧 ПРИМЕРЫ ИНТЕГРАЦИИ ОПТИМИЗИРОВАННОГО REDIS КЛИЕНТА

const { RedisService, RedisClientFactory, RetryManager } = require('./redis-client-optimized');

// 1. EXPRESS.JS ИНТЕГРАЦИЯ
const express = require('express');
const app = express();

// Конфигурация Redis
const redisConfig = {
  host: 'localhost',
  port: 6379,
  db: 0,
  // Дополнительные настройки для стабильности
  connectTimeout: 10000,
  commandTimeout: 5000,
  keepAlive: 30000,
  family: 4,
  maxRetriesPerRequest: 3,
  retryDelayOnFailover: 100,
  enableReadyCheck: false,
  lazyConnect: true
};

// Создание Redis сервиса
const redisService = new RedisService(redisConfig);

// Middleware для Redis
app.use((req, res, next) => {
  req.redis = redisService;
  next();
});

// Примеры роутов с оптимизированным Redis
app.get('/api/data/:key', async (req, res) => {
  try {
    const { key } = req.params;
    
    // Использование retry логики
    const value = await RetryManager.executeWithRetry(async () => {
      return await req.redis.executeCommand('get', key);
    });
    
    res.json({ key, value });
  } catch (error) {
    console.error('API Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/data', async (req, res) => {
  try {
    const { key, value } = req.body;
    
    await RetryManager.executeWithRetry(async () => {
      return await req.redis.executeCommand('set', key, value);
    });
    
    res.json({ success: true });
  } catch (error) {
    console.error('API Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// 2. NESTJS ИНТЕГРАЦИЯ
class RedisModule {
  static forRoot(config) {
    return {
      module: RedisModule,
      providers: [
        {
          provide: 'REDIS_SERVICE',
          useFactory: () => new RedisService(config),
        },
      ],
      exports: ['REDIS_SERVICE'],
    };
  }
}

// NestJS Service
class DataService {
  constructor(@Inject('REDIS_SERVICE') private redisService: RedisService) {}

  async getData(key: string) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redisService.executeCommand('get', key);
    });
  }

  async setData(key: string, value: string) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redisService.executeCommand('set', key, value);
    });
  }
}

// 3. SOCKET.IO ИНТЕГРАЦИЯ
const io = require('socket.io')(server);

// Redis для Socket.IO
const redisAdapter = require('socket.io-redis');
const redisClient = RedisClientFactory.createClient(redisConfig);

io.adapter(redisAdapter({ 
  pubClient: redisClient,
  subClient: redisClient.duplicate()
}));

// Socket.IO с Redis
io.on('connection', (socket) => {
  socket.on('getData', async (data) => {
    try {
      const result = await RetryManager.executeWithRetry(async () => {
        return await redisService.executeCommand('get', data.key);
      });
      
      socket.emit('dataResponse', { key: data.key, value: result });
    } catch (error) {
      socket.emit('error', { message: 'Failed to get data' });
    }
  });
});

// 4. BULL QUEUE ИНТЕГРАЦИЯ
const Queue = require('bull');
const redisService = new RedisService(redisConfig);

// Создание очереди с оптимизированным Redis
const emailQueue = new Queue('email processing', {
  redis: {
    host: 'localhost',
    port: 6379,
    db: 0,
    // Настройки для стабильности
    connectTimeout: 10000,
    commandTimeout: 5000,
    keepAlive: 30000,
    maxRetriesPerRequest: 3,
    retryDelayOnFailover: 100,
    enableReadyCheck: false,
    lazyConnect: true
  }
});

// Обработка задач с retry логикой
emailQueue.process(async (job) => {
  return await RetryManager.executeWithRetry(async () => {
    // Логика обработки email
    console.log('Processing email:', job.data);
    return { success: true };
  });
});

// 5. CACHING MIDDLEWARE
class CacheMiddleware {
  constructor(redisService, ttl = 3600) {
    this.redis = redisService;
    this.ttl = ttl;
  }

  async cache(key, data) {
    try {
      await RetryManager.executeWithRetry(async () => {
        return await this.redis.executeCommand('setex', key, this.ttl, JSON.stringify(data));
      });
    } catch (error) {
      console.error('Cache error:', error);
    }
  }

  async get(key) {
    try {
      const result = await RetryManager.executeWithRetry(async () => {
        return await this.redis.executeCommand('get', key);
      });
      
      return result ? JSON.parse(result) : null;
    } catch (error) {
      console.error('Cache get error:', error);
      return null;
    }
  }

  async invalidate(pattern) {
    try {
      const keys = await RetryManager.executeWithRetry(async () => {
        return await this.redis.executeCommand('keys', pattern);
      });
      
      if (keys.length > 0) {
        await RetryManager.executeWithRetry(async () => {
          return await this.redis.executeCommand('del', ...keys);
        });
      }
    } catch (error) {
      console.error('Cache invalidation error:', error);
    }
  }
}

// 6. RATE LIMITING
class RateLimiter {
  constructor(redisService) {
    this.redis = redisService;
  }

  async checkLimit(key, limit, window) {
    try {
      const current = await RetryManager.executeWithRetry(async () => {
        return await this.redis.executeCommand('incr', key);
      });
      
      if (current === 1) {
        await RetryManager.executeWithRetry(async () => {
          return await this.redis.executeCommand('expire', key, window);
        });
      }
      
      return current <= limit;
    } catch (error) {
      console.error('Rate limit error:', error);
      return true; // В случае ошибки разрешаем запрос
    }
  }
}

// 7. SESSION STORE
class SessionStore {
  constructor(redisService, ttl = 86400) {
    this.redis = redisService;
    this.ttl = ttl;
  }

  async get(sessionId) {
    try {
      const result = await RetryManager.executeWithRetry(async () => {
        return await this.redis.executeCommand('get', `session:${sessionId}`);
      });
      
      return result ? JSON.parse(result) : null;
    } catch (error) {
      console.error('Session get error:', error);
      return null;
    }
  }

  async set(sessionId, sessionData) {
    try {
      await RetryManager.executeWithRetry(async () => {
        return await this.redis.executeCommand('setex', `session:${sessionId}`, this.ttl, JSON.stringify(sessionData));
      });
    } catch (error) {
      console.error('Session set error:', error);
    }
  }

  async destroy(sessionId) {
    try {
      await RetryManager.executeWithRetry(async () => {
        return await this.redis.executeCommand('del', `session:${sessionId}`);
      });
    } catch (error) {
      console.error('Session destroy error:', error);
    }
  }
}

// 8. HEALTH CHECK
class HealthChecker {
  constructor(redisService) {
    this.redis = redisService;
  }

  async checkHealth() {
    try {
      const start = Date.now();
      await this.redis.executeCommand('ping');
      const latency = Date.now() - start;
      
      return {
        status: 'healthy',
        latency: `${latency}ms`,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }
}

// Экспорт всех классов
module.exports = {
  RedisModule,
  DataService,
  CacheMiddleware,
  RateLimiter,
  SessionStore,
  HealthChecker
};

// Пример использования в package.json
/*
{
  "scripts": {
    "start": "node --max-old-space-size=4096 --max-listeners=20 app.js",
    "dev": "node --max-old-space-size=4096 --max-listeners=20 --trace-warnings app.js"
  }
}
*/
