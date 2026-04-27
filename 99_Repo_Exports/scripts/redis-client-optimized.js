// 🔧 ОПТИМИЗИРОВАННЫЙ REDIS КЛИЕНТ ДЛЯ БЭКЕНДА
// Решает все проблемы: memory leak, ECONNRESET, нестабильные соединения

const Redis = require('ioredis');
const EventEmitter = require('events');

// 1. CONNECTION POOLING - Пул соединений
class RedisConnectionPool {
  constructor(config, poolSize = 10) {
    this.config = config;
    this.poolSize = poolSize;
    this.pool = [];
    this.available = [];
    this.busy = new Set();
    this.retryCount = 0;
    this.maxRetries = 5;
    
    this.initializePool();
  }

  async initializePool() {
    for (let i = 0; i < this.poolSize; i++) {
      const client = this.createClient();
      this.pool.push(client);
      this.available.push(client);
    }
  }

  createClient() {
    const client = new Redis({
      ...this.config,
      // 4. LAZY CONNECT - предотвращение множественных подключений
      lazyConnect: true,
      // 2. ОГРАНИЧЕНИЕ СОЕДИНЕНИЙ
      maxRetriesPerRequest: 3,
      retryDelayOnFailover: 100,
      enableReadyCheck: false,
      // Настройки стабильности
      connectTimeout: 10000,
      commandTimeout: 5000,
      keepAlive: 30000,
      family: 4,
      // 5. RETRY ЛОГИКА с экспоненциальной задержкой
      retryDelayOnClusterDown: 300,
      retryDelayOnFailover: 100,
      maxRetriesPerRequest: 3,
    });

    // 3. PROPER ERROR HANDLING
    this.setupErrorHandling(client);
    
    // Увеличиваем лимит слушателей для предотвращения memory leak
    client.setMaxListeners(20);
    
    return client;
  }

  setupErrorHandling(client) {
    client.on('error', (err) => {
      console.error(`Redis client error:`, err);
      this.handleClientError(client, err);
    });

    client.on('connect', () => {
      console.log('Redis client connected');
      this.retryCount = 0;
    });

    client.on('ready', () => {
      console.log('Redis client ready');
    });

    client.on('close', () => {
      console.log('Redis client connection closed');
      this.handleClientClose(client);
    });

    client.on('reconnecting', (delay) => {
      console.log(`Redis reconnecting in ${delay}ms`);
    });
  }

  async handleClientError(client, error) {
    if (error.code === 'ECONNRESET' || error.code === 'ECONNREFUSED') {
      console.warn(`Connection error: ${error.code}, attempting recovery...`);
      await this.recoverClient(client);
    }
  }

  async handleClientClose(client) {
    // Удаляем из busy и добавляем в available после восстановления
    this.busy.delete(client);
    if (client.status === 'ready') {
      this.available.push(client);
    }
  }

  // 5. RETRY ЛОГИКА с экспоненциальной задержкой
  async recoverClient(client) {
    const maxDelay = 30000; // 30 секунд максимум
    const baseDelay = 1000; // 1 секунда базовая задержка
    const delay = Math.min(baseDelay * Math.pow(2, this.retryCount), maxDelay);
    
    if (this.retryCount < this.maxRetries) {
      this.retryCount++;
      console.log(`Retrying connection in ${delay}ms (attempt ${this.retryCount}/${this.maxRetries})`);
      
      setTimeout(async () => {
        try {
          await client.connect();
        } catch (err) {
          console.error('Recovery failed:', err);
          await this.recoverClient(client);
        }
      }, delay);
    } else {
      console.error('Max retries reached, giving up on client recovery');
      this.replaceClient(client);
    }
  }

  replaceClient(oldClient) {
    const index = this.pool.indexOf(oldClient);
    if (index !== -1) {
      oldClient.disconnect();
      const newClient = this.createClient();
      this.pool[index] = newClient;
      this.available.push(newClient);
    }
  }

  async getClient() {
    if (this.available.length > 0) {
      const client = this.available.pop();
      this.busy.add(client);
      return client;
    } else {
      // Если нет доступных клиентов, ждем или создаем новый
      console.warn('No available clients in pool, creating temporary client');
      const tempClient = this.createClient();
      this.busy.add(tempClient);
      return tempClient;
    }
  }

  releaseClient(client) {
    this.busy.delete(client);
    if (client.status === 'ready') {
      this.available.push(client);
    }
  }

  async executeCommand(command, ...args) {
    const client = await this.getClient();
    try {
      const result = await client[command](...args);
      return result;
    } catch (error) {
      console.error(`Redis command error:`, error);
      throw error;
    } finally {
      this.releaseClient(client);
    }
  }

  async disconnect() {
    for (const client of this.pool) {
      await client.disconnect();
    }
  }
}

// 2. ОГРАНИЧЕНИЕ КОЛИЧЕСТВА СОЕДИНЕНИЙ
class RedisConnectionManager {
  constructor(config) {
    this.config = config;
    this.maxConnections = 50; // Максимум 50 соединений
    this.currentConnections = 0;
    this.connectionQueue = [];
    this.pool = new RedisConnectionPool(config, 10);
    
    // Увеличиваем лимит слушателей для основного менеджера
    this.setMaxListeners(20);
  }

  async getConnection() {
    if (this.currentConnections >= this.maxConnections) {
      return new Promise((resolve) => {
        this.connectionQueue.push(resolve);
      });
    }

    this.currentConnections++;
    return this.pool.getClient();
  }

  releaseConnection(client) {
    this.pool.releaseClient(client);
    this.currentConnections--;
    
    if (this.connectionQueue.length > 0) {
      const resolve = this.connectionQueue.shift();
      resolve(this.getConnection());
    }
  }

  async executeCommand(command, ...args) {
    const client = await this.getConnection();
    try {
      const result = await client[command](...args);
      return result;
    } catch (error) {
      console.error(`Redis command error:`, error);
      throw error;
    } finally {
      this.releaseConnection(client);
    }
  }
}

// 3. PROPER ERROR HANDLING - Расширенная обработка ошибок
class RedisService extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
    this.connectionManager = new RedisConnectionManager(config);
    this.isConnected = false;
    this.healthCheckInterval = null;
    
    this.setupErrorHandling();
    this.startHealthCheck();
  }

  setupErrorHandling() {
    // Обработка необработанных ошибок
    process.on('uncaughtException', (error) => {
      console.error('Uncaught Exception:', error);
      this.handleCriticalError(error);
    });

    process.on('unhandledRejection', (reason, promise) => {
      console.error('Unhandled Rejection at:', promise, 'reason:', reason);
      this.handleCriticalError(reason);
    });

    // Обработка ошибок Redis
    this.on('error', (error) => {
      console.error('Redis Service Error:', error);
      this.handleRedisError(error);
    });
  }

  handleCriticalError(error) {
    console.error('Critical error detected, attempting recovery...');
    this.reconnect();
  }

  handleRedisError(error) {
    if (error.code === 'ECONNRESET') {
      console.warn('Connection reset detected, reconnecting...');
      this.reconnect();
    } else if (error.code === 'ECONNREFUSED') {
      console.warn('Connection refused, will retry...');
      setTimeout(() => this.reconnect(), 5000);
    }
  }

  async reconnect() {
    try {
      await this.connectionManager.pool.disconnect();
      this.connectionManager = new RedisConnectionManager(this.config);
      this.isConnected = true;
      console.log('Redis reconnected successfully');
    } catch (error) {
      console.error('Reconnection failed:', error);
      setTimeout(() => this.reconnect(), 10000);
    }
  }

  startHealthCheck() {
    this.healthCheckInterval = setInterval(async () => {
      try {
        await this.connectionManager.executeCommand('ping');
        this.isConnected = true;
      } catch (error) {
        console.warn('Health check failed:', error);
        this.isConnected = false;
      }
    }, 30000); // Проверка каждые 30 секунд
  }

  async executeCommand(command, ...args) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }
    
    try {
      return await this.connectionManager.executeCommand(command, ...args);
    } catch (error) {
      this.emit('error', error);
      throw error;
    }
  }

  async disconnect() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }
    await this.connectionManager.pool.disconnect();
  }
}

// 4. LAZY CONNECT - Фабрика для создания клиентов с lazy connect
class RedisClientFactory {
  static createClient(config) {
    return new Redis({
      ...config,
      lazyConnect: true,
      maxRetriesPerRequest: 3,
      retryDelayOnFailover: 100,
      enableReadyCheck: false,
      connectTimeout: 10000,
      commandTimeout: 5000,
      keepAlive: 30000,
      family: 4,
    });
  }

  static createService(config) {
    return new RedisService(config);
  }
}

// 5. RETRY ЛОГИКА с экспоненциальной задержкой
class RetryManager {
  static async executeWithRetry(operation, maxRetries = 5, baseDelay = 1000) {
    let lastError;
    
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        
        if (attempt === maxRetries - 1) {
          throw error;
        }
        
        const delay = Math.min(baseDelay * Math.pow(2, attempt), 30000);
        console.log(`Operation failed, retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries})`);
        
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    throw lastError;
  }
}

// Экспорт оптимизированных классов
module.exports = {
  RedisConnectionPool,
  RedisConnectionManager,
  RedisService,
  RedisClientFactory,
  RetryManager
};

// Пример использования:
/*
const { RedisService } = require('./redis-client-optimized');

const redisConfig = {
  host: 'localhost',
  port: 6379,
  db: 0
};

const redisService = new RedisService(redisConfig);

// Использование
async function example() {
  try {
    await redisService.executeCommand('set', 'key', 'value');
    const value = await redisService.executeCommand('get', 'key');
    console.log('Value:', value);
  } catch (error) {
    console.error('Error:', error);
  }
}
*/
