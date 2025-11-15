require('dotenv').config();
const express = require('express');
const compression = require('compression');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const RedpandaProducer = require('./src/kafka/producer');
const webhooksRouter = require('./src/routes/webhooks');

const app = express();
const PORT = process.env.PORT || 3000;

// Inicializa o producer do Redpanda
const producer = new RedpandaProducer();

// Disponibiliza o producer para as rotas
app.set('producer', producer);

// ===== MIDDLEWARES DE PRODUCAO =====

// Seguranca: Helmet (protege contra vulnerabilidades comuns)
app.use(helmet());

// Compressao de respostas (reduz bandwidth)
app.use(compression());

// Rate limiting global (protege contra abuse)
const limiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1 minuto
  max: 1000, // maximo de 1000 requisicoes por minuto por IP
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Muitas requisicoes. Tente novamente em alguns instantes.' },
});
app.use(limiter);

// Parse de JSON com limite de tamanho (previne payload muito grande)
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Timeout global para requisicoes (30 segundos)
app.use((req, res, next) => {
  req.setTimeout(30000);
  res.setTimeout(30000);
  next();
});

// Keep-Alive para conexoes HTTP
app.use((req, res, next) => {
  res.setHeader('Connection', 'keep-alive');
  next();
});

// ===== ROTAS =====

// Health check melhorado (verifica conexao com Kafka)
app.get('/health', (req, res) => {
  const producerStatus = producer.getStatus();
  const isHealthy = producer.isHealthy();

  const status = isHealthy ? 'healthy' : 'unhealthy';
  const httpStatus = isHealthy ? 200 : 503;

  res.status(httpStatus).json({
    status,
    service: 'webhook-messages',
    timestamp: new Date().toISOString(),
    kafka: {
      connected: producerStatus.connected,
      connecting: producerStatus.connecting,
    },
    uptime: process.uptime(),
    memory: {
      used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB',
      total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024) + 'MB',
    },
  });
});

// Rotas de webhook
app.use('/v1', webhooksRouter);

// Tratamento de rotas nao encontradas
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: 'Rota nao encontrada',
  });
});

// ===== INICIALIZACAO DO SERVIDOR =====

let server;

async function startServer() {
  try {
    // Conecta ao Redpanda
    await producer.connect();

    // Inicia o servidor Express
    server = app.listen(PORT, () => {
      console.log(`\n${'='.repeat(50)}`);
      console.log(`>> Servidor rodando na porta ${PORT}`);
      console.log(`>> Modo: ${process.env.NODE_ENV || 'development'}`);
      console.log(`>> PID: ${process.pid}`);
      console.log(`>> Webhook endpoint: http://localhost:${PORT}/v1/evolution-messages/:uuid`);
      console.log(`>> Health check: http://localhost:${PORT}/health`);
      console.log(`${'='.repeat(50)}\n`);

      // Notifica o PM2 que a aplicacao esta pronta
      if (process.send) {
        process.send('ready');
      }
    });

    // Configura keep-alive no servidor HTTP
    server.keepAliveTimeout = 65000; // 65 segundos
    server.headersTimeout = 66000; // 66 segundos (maior que keepAliveTimeout)

  } catch (error) {
    console.error('[ERRO] Falha ao iniciar o servidor:', error);
    process.exit(1);
  }
}

// ===== GRACEFUL SHUTDOWN =====

async function gracefulShutdown(signal) {
  console.log(`\n[INFO] Sinal ${signal} recebido. Iniciando graceful shutdown...`);

  // Para de aceitar novas conexoes
  if (server) {
    server.close(() => {
      console.log('[INFO] Servidor HTTP fechado');
    });
  }

  try {
    // Desconecta do Redpanda
    await producer.disconnect();
    console.log('[INFO] Desconectado do Redpanda');

    console.log('[OK] Graceful shutdown concluido');
    process.exit(0);
  } catch (error) {
    console.error('[ERRO] Erro durante graceful shutdown:', error);
    process.exit(1);
  }
}

// Tratamento de sinais de encerramento
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Tratamento de erros nao capturados
process.on('uncaughtException', (error) => {
  console.error('[ERRO] Uncaught Exception:', error);
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[ERRO] Unhandled Rejection at:', promise, 'reason:', reason);
  gracefulShutdown('unhandledRejection');
});

// Inicia o servidor
startServer();
