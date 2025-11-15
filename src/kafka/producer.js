const { Kafka, Partitioners, logLevel } = require('kafkajs');

class RedpandaProducer {
  constructor() {
    this.kafka = new Kafka({
      clientId: 'webhook-messages-api',
      brokers: [`${process.env.REDPANDA_HOST}:${process.env.REDPANDA_PORT}`],
      sasl: {
        mechanism: 'scram-sha-256',
        username: process.env.REDPANDA_USERNAME,
        password: process.env.REDPANDA_PASSWORD,
      },
      ssl: false,
      // Configuracoes de retry e timeout
      connectionTimeout: 10000,
      requestTimeout: 30000,
      retry: {
        initialRetryTime: 300,
        retries: 8,
        maxRetryTime: 30000,
        multiplier: 2,
        factor: 0.2,
      },
      logLevel: process.env.NODE_ENV === 'production' ? logLevel.ERROR : logLevel.INFO,
    });

    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
      // Configuracoes de producao para alta performance e confiabilidade
      idempotent: true,
      maxInFlightRequests: 5,
      transactionalId: undefined,
      allowAutoTopicCreation: false,
      retry: {
        retries: 5,
        initialRetryTime: 300,
        maxRetryTime: 30000,
      },
    });

    this.isConnected = false;
    this.isConnecting = false;
    this.setupEventListeners();
  }

  setupEventListeners() {
    // Event listeners para monitorar a saude da conexao
    this.producer.on('producer.connect', () => {
      console.log('[OK] Producer conectado ao Redpanda');
      this.isConnected = true;
      this.isConnecting = false;
    });

    this.producer.on('producer.disconnect', () => {
      console.log('[WARN] Producer desconectado do Redpanda');
      this.isConnected = false;
    });

    this.producer.on('producer.network.request_timeout', (payload) => {
      console.error('[ERRO] Timeout na requisicao ao Redpanda:', payload);
    });
  }

  async connect() {
    if (this.isConnected) {
      console.log('[INFO] Producer ja esta conectado');
      return;
    }

    if (this.isConnecting) {
      console.log('[INFO] Conexao ja esta em andamento');
      return;
    }

    this.isConnecting = true;

    try {
      await this.producer.connect();
      this.isConnected = true;
      this.isConnecting = false;
      console.log('[OK] Conectado ao Redpanda com sucesso');
    } catch (error) {
      this.isConnecting = false;
      console.error('[ERRO] Erro ao conectar ao Redpanda:', error);
      throw error;
    }
  }

  async sendMessage(topic, message) {
    // Tenta reconectar se nao estiver conectado
    if (!this.isConnected && !this.isConnecting) {
      console.log('[WARN] Producer desconectado. Tentando reconectar...');
      try {
        await this.connect();
      } catch (error) {
        throw new Error('Falha ao reconectar ao Redpanda: ' + error.message);
      }
    }

    try {
      const result = await this.producer.send({
        topic,
        messages: [
          {
            value: JSON.stringify(message),
            timestamp: Date.now().toString(),
          },
        ],
        // Timeout por mensagem
        timeout: 30000,
        // Compression para economizar bandwidth
        compression: 1, // GZIP
        // Aguardar confirmacao de todas as replicas
        acks: -1,
      });

      console.log(`[OK] Mensagem enviada para o topico ${topic}`);
      return result;
    } catch (error) {
      console.error(`[ERRO] Erro ao enviar mensagem para o topico ${topic}:`, error);
      throw error;
    }
  }

  // Metodo para verificar saude do producer (usado no healthcheck)
  isHealthy() {
    return this.isConnected;
  }

  // Metodo para obter metricas basicas
  getStatus() {
    return {
      connected: this.isConnected,
      connecting: this.isConnecting,
    };
  }

  async disconnect() {
    try {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log('[OK] Desconectado do Redpanda');
    } catch (error) {
      console.error('[ERRO] Erro ao desconectar do Redpanda:', error);
      throw error;
    }
  }
}

module.exports = RedpandaProducer;
