// 🔧 REDIS КЛИЕНТ ДЛЯ ИСПРАВЛЕНИЯ ТАЙМАУТОВ
// Специально оптимизирован для быстрого выполнения команд

const Redis = require('ioredis');

class RedisTimeoutClient {
  constructor(config) {
    this.config = {
      host: 'localhost',
      port: 6379,
      db: 0,
      // КРИТИЧЕСКИ ВАЖНЫЕ настройки для предотвращения таймаутов
      connectTimeout: 5000,        // 5 секунд на подключение
      commandTimeout: 2000,        // 2 секунды на команду
      lazyConnect: true,           // Подключение только при необходимости
      maxRetriesPerRequest: 1,     // Только 1 попытка
      retryDelayOnFailover: 100,   // Быстрая повторная попытка
      enableReadyCheck: false,     // Отключить проверку готовности
      keepAlive: 30000,            // 30 секунд keepalive
      family: 4,                   // IPv4
      // Дополнительные настройки для стабильности
      enableOfflineQueue: false,   // Отключить офлайн очередь
      maxMemoryPolicy: 'allkeys-lru',
      maxMemory: '16gb',
      ...config
    };
    
    this.client = null;
    this.isConnected = false;
    this.retryCount = 0;
    this.maxRetries = 3;
    
    this.setupErrorHandling();
  }

  setupErrorHandling() {
    // Увеличиваем лимит слушателей
    this.setMaxListeners(20);
  }

  async connect() {
    if (this.client && this.isConnected) {
      return this.client;
    }

    try {
      this.client = new Redis(this.config);
      
      this.client.on('error', (err) => {
        console.error('Redis client error:', err);
        this.isConnected = false;
        this.handleError(err);
      });

      this.client.on('connect', () => {
        console.log('Redis client connected');
        this.isConnected = true;
        this.retryCount = 0;
      });

      this.client.on('ready', () => {
        console.log('Redis client ready');
        this.isConnected = true;
      });

      this.client.on('close', () => {
        console.log('Redis client connection closed');
        this.isConnected = false;
      });

      this.client.on('reconnecting', (delay) => {
        console.log(`Redis reconnecting in ${delay}ms`);
      });

      // Подключаемся только если lazyConnect отключен
      if (!this.config.lazyConnect) {
        await this.client.connect();
      }

      return this.client;
    } catch (error) {
      console.error('Failed to create Redis client:', error);
      throw error;
    }
  }

  async handleError(error) {
    if (error.code === 'ECONNRESET' || error.code === 'ECONNREFUSED') {
      console.warn(`Connection error: ${error.code}, attempting recovery...`);
      await this.reconnect();
    }
  }

  async reconnect() {
    if (this.retryCount >= this.maxRetries) {
      console.error('Max retries reached, giving up');
      return;
    }

    this.retryCount++;
    const delay = Math.min(1000 * Math.pow(2, this.retryCount), 10000);
    
    console.log(`Retrying connection in ${delay}ms (attempt ${this.retryCount}/${this.maxRetries})`);
    
    setTimeout(async () => {
      try {
        if (this.client) {
          await this.client.disconnect();
        }
        await this.connect();
      } catch (error) {
        console.error('Reconnection failed:', error);
        await this.reconnect();
      }
    }, delay);
  }

  async executeCommand(command, ...args) {
    try {
      if (!this.client) {
        await this.connect();
      }

      // Проверяем подключение
      if (!this.isConnected) {
        await this.connect();
      }

      // Выполняем команду с таймаутом
      const result = await Promise.race([
        this.client[command](...args),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Command timeout')), this.config.commandTimeout)
        )
      ]);

      return result;
    } catch (error) {
      console.error(`Redis command error (${command}):`, error);
      
      if (error.message === 'Command timeout') {
        console.warn(`Command ${command} timed out after ${this.config.commandTimeout}ms`);
      }
      
      throw error;
    }
  }

  // Специальные методы для стримов
  async checkStreamExists(streamName) {
    try {
      const result = await this.executeCommand('exists', streamName);
      return result === 1;
    } catch (error) {
      console.error(`Error checking stream ${streamName}:`, error);
      return false;
    }
  }

  async readStreams(streams, count = 10) {
    try {
      const result = await this.executeCommand('xread', 'COUNT', count, 'STREAMS', ...streams, ...streams.map(() => '$'));
      return result;
    } catch (error) {
      console.error('Error reading streams:', error);
      throw error;
    }
  }

  async addToStream(streamName, data) {
    try {
      const result = await this.executeCommand('xadd', streamName, '*', ...Object.entries(data).flat());
      return result;
    } catch (error) {
      console.error(`Error adding to stream ${streamName}:`, error);
      throw error;
    }
  }

  async getStreamLength(streamName) {
    try {
      const result = await this.executeCommand('xlen', streamName);
      return result;
    } catch (error) {
      console.error(`Error getting stream length ${streamName}:`, error);
      return 0;
    }
  }

  // Методы для работы с ключами
  async exists(key) {
    try {
      const result = await this.executeCommand('exists', key);
      return result === 1;
    } catch (error) {
      console.error(`Error checking key ${key}:`, error);
      return false;
    }
  }

  async get(key) {
    try {
      const result = await this.executeCommand('get', key);
      return result;
    } catch (error) {
      console.error(`Error getting key ${key}:`, error);
      return null;
    }
  }

  async set(key, value, ttl = null) {
    try {
      if (ttl) {
        const result = await this.executeCommand('setex', key, ttl, value);
        return result;
      } else {
        const result = await this.executeCommand('set', key, value);
        return result;
      }
    } catch (error) {
      console.error(`Error setting key ${key}:`, error);
      throw error;
    }
  }

  async del(key) {
    try {
      const result = await this.executeCommand('del', key);
      return result;
    } catch (error) {
      console.error(`Error deleting key ${key}:`, error);
      throw error;
    }
  }

  // Методы для работы с хешами
  async hget(hashKey, field) {
    try {
      const result = await this.executeCommand('hget', hashKey, field);
      return result;
    } catch (error) {
      console.error(`Error getting hash field ${hashKey}.${field}:`, error);
      return null;
    }
  }

  async hset(hashKey, field, value) {
    try {
      const result = await this.executeCommand('hset', hashKey, field, value);
      return result;
    } catch (error) {
      console.error(`Error setting hash field ${hashKey}.${field}:`, error);
      throw error;
    }
  }

  async hgetall(hashKey) {
    try {
      const result = await this.executeCommand('hgetall', hashKey);
      return result;
    } catch (error) {
      console.error(`Error getting hash ${hashKey}:`, error);
      return {};
    }
  }

  // Методы для работы со списками
  async lpush(listKey, ...values) {
    try {
      const result = await this.executeCommand('lpush', listKey, ...values);
      return result;
    } catch (error) {
      console.error(`Error pushing to list ${listKey}:`, error);
      throw error;
    }
  }

  async rpop(listKey) {
    try {
      const result = await this.executeCommand('rpop', listKey);
      return result;
    } catch (error) {
      console.error(`Error popping from list ${listKey}:`, error);
      return null;
    }
  }

  async llen(listKey) {
    try {
      const result = await this.executeCommand('llen', listKey);
      return result;
    } catch (error) {
      console.error(`Error getting list length ${listKey}:`, error);
      return 0;
    }
  }

  // Методы для работы с множествами
  async sadd(setKey, ...members) {
    try {
      const result = await this.executeCommand('sadd', setKey, ...members);
      return result;
    } catch (error) {
      console.error(`Error adding to set ${setKey}:`, error);
      throw error;
    }
  }

  async smembers(setKey) {
    try {
      const result = await this.executeCommand('smembers', setKey);
      return result;
    } catch (error) {
      console.error(`Error getting set members ${setKey}:`, error);
      return [];
    }
  }

  // Методы для работы с отсортированными множествами
  async zadd(zsetKey, score, member) {
    try {
      const result = await this.executeCommand('zadd', zsetKey, score, member);
      return result;
    } catch (error) {
      console.error(`Error adding to sorted set ${zsetKey}:`, error);
      throw error;
    }
  }

  async zrange(zsetKey, start = 0, stop = -1) {
    try {
      const result = await this.executeCommand('zrange', zsetKey, start, stop);
      return result;
    } catch (error) {
      console.error(`Error getting sorted set range ${zsetKey}:`, error);
      return [];
    }
  }

  // Публикация/подписка
  async publish(channel, message) {
    try {
      const result = await this.executeCommand('publish', channel, message);
      return result;
    } catch (error) {
      console.error(`Error publishing to channel ${channel}:`, error);
      throw error;
    }
  }

  // Атомарные операции
  async incr(key) {
    try {
      const result = await this.executeCommand('incr', key);
      return result;
    } catch (error) {
      console.error(`Error incrementing key ${key}:`, error);
      throw error;
    }
  }

  async decr(key) {
    try {
      const result = await this.executeCommand('decr', key);
      return result;
    } catch (error) {
      console.error(`Error decrementing key ${key}:`, error);
      throw error;
    }
  }

  // TTL операции
  async expire(key, seconds) {
    try {
      const result = await this.executeCommand('expire', key, seconds);
      return result;
    } catch (error) {
      console.error(`Error setting TTL for key ${key}:`, error);
      throw error;
    }
  }

  async ttl(key) {
    try {
      const result = await this.executeCommand('ttl', key);
      return result;
    } catch (error) {
      console.error(`Error getting TTL for key ${key}:`, error);
      return -1;
    }
  }

  // Поиск ключей
  async keys(pattern) {
    try {
      const result = await this.executeCommand('keys', pattern);
      return result;
    } catch (error) {
      console.error(`Error searching keys with pattern ${pattern}:`, error);
      return [];
    }
  }

  // Получение информации о ключе
  async type(key) {
    try {
      const result = await this.executeCommand('type', key);
      return result;
    } catch (error) {
      console.error(`Error getting type for key ${key}:`, error);
      return 'none';
    }
  }

  // Проверка здоровья
  async ping() {
    try {
      const result = await this.executeCommand('ping');
      return result === 'PONG';
    } catch (error) {
      console.error('Redis ping failed:', error);
      return false;
    }
  }

  // Получение информации о сервере
  async info(section = null) {
    try {
      const result = await this.executeCommand('info', section || '');
      return result;
    } catch (error) {
      console.error('Error getting Redis info:', error);
      return null;
    }
  }

  // Отключение
  async disconnect() {
    try {
      if (this.client) {
        await this.client.disconnect();
        this.client = null;
        this.isConnected = false;
        console.log('Redis client disconnected');
      }
    } catch (error) {
      console.error('Error disconnecting Redis client:', error);
    }
  }
}

// Фабрика для создания клиентов
class RedisTimeoutClientFactory {
  static createClient(config = {}) {
    return new RedisTimeoutClient(config);
  }

  static createClientWithDefaults(host = 'localhost', port = 6379, db = 0) {
    return new RedisTimeoutClient({ host, port, db });
  }
}

module.exports = {
  RedisTimeoutClient,
  RedisTimeoutClientFactory
};

// Пример использования:
/*
const { RedisTimeoutClientFactory } = require('./redis-timeout-client');

const redis = RedisTimeoutClientFactory.createClientWithDefaults();

async function example() {
  try {
    // Проверка подключения
    const isConnected = await redis.ping();
    console.log('Redis connected:', isConnected);

    // Работа со стримами
    const streamExists = await redis.checkStreamExists('stream:volatility');
    console.log('Stream exists:', streamExists);

    // Добавление в стрим
    await redis.addToStream('stream:volatility', { 
      price: '100.50', 
      timestamp: Date.now() 
    });

    // Чтение стримов
    const streams = await redis.readStreams(['stream:volatility', 'stream:top-gainers']);
    console.log('Streams data:', streams);

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await redis.disconnect();
  }
}
*/
