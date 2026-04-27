// 🚀 ПРИМЕР ИСПОЛЬЗОВАНИЯ ОПТИМИЗИРОВАННОГО REDIS КЛИЕНТА

const { createRedisService, checkRedisConnection, monitorRedis } = require('./redis-config');
const { RetryManager } = require('./redis-client-optimized');

// Создание Redis сервиса
const redisService = createRedisService(process.env.NODE_ENV || 'development');

// Глобальная переменная для graceful shutdown
global.redisService = redisService;

// Основной класс приложения
class App {
  constructor() {
    this.redis = redisService;
    this.isReady = false;
  }

  async start() {
    console.log('🚀 Starting application...');
    
    // Проверка подключения к Redis
    const isConnected = await checkRedisConnection(this.redis);
    if (!isConnected) {
      throw new Error('Failed to connect to Redis');
    }
    
    this.isReady = true;
    console.log('✅ Application started successfully');
    
    // Запуск мониторинга
    this.startMonitoring();
  }

  async startMonitoring() {
    setInterval(async () => {
      const stats = await monitorRedis(this.redis);
      if (stats) {
        console.log('📊 Redis Stats:', {
          connected_clients: stats.clients.connected_clients,
          used_memory_human: stats.memory.used_memory_human,
          uptime_in_seconds: stats.server.uptime_in_seconds
        });
      }
    }, 60000); // Каждую минуту
  }

  // Примеры использования с retry логикой
  async setData(key, value, ttl = null) {
    return await RetryManager.executeWithRetry(async () => {
      if (ttl) {
        return await this.redis.executeCommand('setex', key, ttl, value);
      } else {
        return await this.redis.executeCommand('set', key, value);
      }
    });
  }

  async getData(key) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('get', key);
    });
  }

  async deleteData(key) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('del', key);
    });
  }

  // Работа со стримами
  async addToStream(streamName, data) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('xadd', streamName, '*', ...Object.entries(data).flat());
    });
  }

  async readFromStream(streamName, count = 10) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('xread', 'COUNT', count, 'STREAMS', streamName, '$');
    });
  }

  // Работа с хешами
  async setHash(hashKey, field, value) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('hset', hashKey, field, value);
    });
  }

  async getHash(hashKey, field = null) {
    return await RetryManager.executeWithRetry(async () => {
      if (field) {
        return await this.redis.executeCommand('hget', hashKey, field);
      } else {
        return await this.redis.executeCommand('hgetall', hashKey);
      }
    });
  }

  // Работа со списками
  async pushToList(listKey, ...values) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('lpush', listKey, ...values);
    });
  }

  async popFromList(listKey) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('rpop', listKey);
    });
  }

  // Работа с множествами
  async addToSet(setKey, ...members) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('sadd', setKey, ...members);
    });
  }

  async getSetMembers(setKey) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('smembers', setKey);
    });
  }

  // Работа с отсортированными множествами
  async addToSortedSet(zsetKey, score, member) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('zadd', zsetKey, score, member);
    });
  }

  async getSortedSetRange(zsetKey, start = 0, stop = -1) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('zrange', zsetKey, start, stop);
    });
  }

  // Публикация/подписка
  async publish(channel, message) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('publish', channel, message);
    });
  }

  // Атомарные операции
  async increment(key, amount = 1) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('incrby', key, amount);
    });
  }

  async decrement(key, amount = 1) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('decrby', key, amount);
    });
  }

  // Работа с TTL
  async setTTL(key, seconds) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('expire', key, seconds);
    });
  }

  async getTTL(key) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('ttl', key);
    });
  }

  // Пакетные операции
  async multiExec(commands) {
    return await RetryManager.executeWithRetry(async () => {
      const multi = this.redis.executeCommand('multi');
      
      for (const command of commands) {
        multi[command[0]](...command.slice(1));
      }
      
      return await multi.exec();
    });
  }

  // Поиск ключей
  async findKeys(pattern) {
    return await RetryManager.executeWithRetry(async () => {
      return await this.redis.executeCommand('keys', pattern);
    });
  }

  // Получение информации о ключе
  async getKeyInfo(key) {
    return await RetryManager.executeWithRetry(async () => {
      const type = await this.redis.executeCommand('type', key);
      const ttl = await this.redis.executeCommand('ttl', key);
      const size = await this.redis.executeCommand('memory', 'usage', key);
      
      return { type, ttl, size };
    });
  }
}

// Пример использования
async function main() {
  const app = new App();
  
  try {
    await app.start();
    
    // Примеры использования
    console.log('📝 Setting data...');
    await app.setData('user:1', JSON.stringify({ name: 'John', age: 30 }), 3600);
    
    console.log('📖 Getting data...');
    const userData = await app.getData('user:1');
    console.log('User data:', JSON.parse(userData));
    
    console.log('📊 Adding to stream...');
    await app.addToStream('events', { type: 'user_login', userId: '1', timestamp: Date.now() });
    
    console.log('📈 Reading from stream...');
    const events = await app.readFromStream('events');
    console.log('Events:', events);
    
    console.log('🔢 Incrementing counter...');
    const count = await app.increment('counter', 5);
    console.log('Counter:', count);
    
    console.log('✅ All operations completed successfully');
    
  } catch (error) {
    console.error('❌ Application error:', error);
    process.exit(1);
  }
}

// Запуск приложения
if (require.main === module) {
  main().catch(console.error);
}

module.exports = App;
