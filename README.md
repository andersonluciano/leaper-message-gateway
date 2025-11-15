# Webhook Messages API

API webhook para receber mensagens de diversos clientes e enviar para topicos no Redpanda (Kafka).

## Caracteristicas

- Recebe mensagens via webhook HTTP POST
- Extrai UUID da URL e adiciona como `companyId` no body
- Envia mensagens para topicos do Redpanda com autenticacao SASL
- Suporte para multiplos webhooks e topicos (extensivel)

## Pre-requisitos

- Node.js >= 14.x
- npm ou yarn
- Acesso a um cluster Redpanda/Kafka

## Instalacao

```bash
npm install
```

## Configuracao

1. Copie o arquivo `.env.example` para `.env`:
```bash
cp .env.example .env
```

2. Configure as variaveis de ambiente no arquivo `.env`:
```env
PORT=3000
REDPANDA_HOST=167.99.117.1
REDPANDA_PORT=9092
REDPANDA_USERNAME=app_user
REDPANDA_PASSWORD=sua_senha
CONSUMER_GROUP=webhook-messages-consumer
```

## Uso

### Desenvolvimento

```bash
npm run dev
```

### Producao

#### Modo simples (sem PM2)

```bash
npm start
```

#### Modo cluster com PM2 (RECOMENDADO)

O PM2 permite escalar a aplicacao em modo cluster, aproveitando todos os nucleos da CPU e garantindo alta disponibilidade.

```bash
# Instalar dependencias (incluindo PM2)
npm install

# Iniciar em modo cluster (usando ecosystem.config.js)
npm run pm2:start

# Verificar status
npm run pm2:monit

# Ver logs em tempo real
npm run pm2:logs

# Reiniciar a aplicacao
npm run pm2:restart

# Parar a aplicacao
npm run pm2:stop

# Remover do PM2
npm run pm2:delete
```

**Comandos PM2 adicionais:**

```bash
# Ver lista de processos
pm2 list

# Monitorar recursos
pm2 monit

# Ver logs
pm2 logs webhook-messages-api

# Zerar logs
pm2 flush

# Salvar configuracao atual (para reiniciar no boot)
pm2 save
pm2 startup
```

## Endpoints

### Webhook Evolution API

**POST** `/v1/evolution-messages/:uuid`

Recebe mensagens da Evolution API e envia para o topico `evolution-messages`.

**Parametros:**
- `uuid` (path): UUID da empresa

**Body:** Qualquer JSON valido

**Exemplo de requisicao:**

```bash
curl -X POST http://localhost:3000/v1/evolution-messages/3e45988c-059e-469d-a8f2-e57533db64a2 \
  -H "Content-Type: application/json" \
  -d '{
    "event": "message.received",
    "data": {
      "from": "5511999999999",
      "body": "Ola!"
    }
  }'
```

**Exemplo de resposta:**

```json
{
  "success": true,
  "message": "Mensagem recebida e enviada para o topico com sucesso",
  "companyId": "3e45988c-059e-469d-a8f2-e57533db64a2"
}
```

**Dados enviados para o Redpanda:**

```json
{
  "event": "message.received",
  "data": {
    "from": "5511999999999",
    "body": "Ola!"
  },
  "companyId": "3e45988c-059e-469d-a8f2-e57533db64a2"
}
```

### Health Check

**GET** `/health`

Verifica o status da API e da conexao com o Redpanda.

```bash
curl http://localhost:3000/health
```

**Exemplo de resposta (healthy):**

```json
{
  "status": "healthy",
  "service": "webhook-messages",
  "timestamp": "2025-11-14T14:30:00.000Z",
  "kafka": {
    "connected": true,
    "connecting": false
  },
  "uptime": 123.45,
  "memory": {
    "used": "45MB",
    "total": "128MB"
  }
}
```

Se o servico estiver com problemas (ex: desconectado do Kafka), retorna status HTTP 503.

## Recursos de Producao e Escalabilidade

Este projeto foi otimizado para ambientes de producao de alta escala. Principais recursos:

### Seguranca

- **Helmet**: Protecao contra vulnerabilidades web comuns (XSS, clickjacking, etc)
- **Rate Limiting**: Limite de 1000 requisicoes por minuto por IP (configuravel)
- **Validacao de Payload**: Limite de 1MB por requisicao (previne DoS)

### Performance e Confiabilidade

- **Compressao GZIP**: Respostas comprimidas para reduzir bandwidth
- **Keep-Alive**: Reutilizacao de conexoes HTTP para melhor performance
- **Connection Pooling**: Producer do Kafka configurado para alta performance
- **Idempotencia**: Mensagens duplicadas sao automaticamente descartadas pelo Kafka
- **Retry Logic**: Tentativas automaticas em caso de falha temporaria
- **Auto-Reconnect**: Reconexao automatica ao Redpanda em caso de queda

### Kafka/Redpanda Otimizado

- **Compression**: Mensagens comprimidas com GZIP antes de enviar
- **Acks**: Aguarda confirmacao de todas as replicas (-1)
- **Batch**: Agrupamento automatico de mensagens para melhor throughput
- **Timeouts**: Configuracoes de timeout apropriadas para producao
- **Retries**: Ate 5 tentativas automaticas com backoff exponencial

### Monitoramento

- **Health Check**: Endpoint `/health` verifica status do servico e conexao Kafka
- **Logs Estruturados**: Logs com timestamp e nivel de severidade
- **Metricas**: Memory usage e uptime disponiveis no health check
- **PM2 Monitoring**: Monitoramento de CPU, memoria e restarts via PM2

### Graceful Shutdown

O servidor fecha conexoes de forma elegante ao receber sinais de encerramento:
- Para de aceitar novas requisicoes
- Aguarda requisicoes em andamento finalizarem
- Desconecta do Redpanda de forma segura
- Suporta SIGTERM, SIGINT e erros nao capturados

### Cluster Mode (PM2)

Configurado para rodar em modo cluster aproveitando todos os nucleos da CPU:
- Auto-restart em caso de falha
- Load balancing automatico entre workers
- Zero-downtime reload
- Logs centralizados
- Max memory restart (1GB por worker)

## Estrutura do Projeto

```
webhook-messages/
├── src/
│   ├── kafka/
│   │   └── producer.js      # Producer do Redpanda/Kafka
│   └── routes/
│       └── webhooks.js      # Rotas dos webhooks
├── logs/                    # Logs do PM2 (criado automaticamente)
├── .env                     # Variaveis de ambiente (nao versionado)
├── .env.example            # Exemplo de configuracao
├── .gitignore
├── ecosystem.config.js      # Configuracao do PM2
├── index.js                # Arquivo principal
├── package.json
└── README.md
```

## Adicionando Novos Webhooks

Para adicionar um novo webhook:

1. Edite o arquivo `src/routes/webhooks.js`:

```javascript
router.post('/novo-webhook/:uuid', async (req, res) => {
  try {
    const { uuid } = req.params;
    const messageData = req.body;

    const enrichedData = {
      ...messageData,
      companyId: uuid,
    };

    const producer = req.app.get('producer');
    await producer.sendMessage('nome-do-topico', enrichedData);

    res.status(200).json({
      success: true,
      message: 'Mensagem recebida e enviada para o topico com sucesso',
      companyId: uuid,
    });
  } catch (error) {
    console.error('Erro ao processar webhook:', error);
    res.status(500).json({
      success: false,
      message: 'Erro ao processar webhook',
      error: error.message,
    });
  }
});
```

## Tecnologias

- **Express**: Framework web
- **KafkaJS**: Cliente Kafka/Redpanda para Node.js
- **dotenv**: Gerenciamento de variaveis de ambiente
- **Helmet**: Seguranca HTTP
- **Compression**: Compressao GZIP
- **Express Rate Limit**: Rate limiting
- **PM2**: Process manager para producao

## Configuracoes Tecnicas

### Autenticacao SASL
O projeto utiliza autenticacao SASL com o mecanismo **SCRAM-SHA-256** para conectar ao Redpanda. Certifique-se de que seu cluster Redpanda esta configurado para suportar este mecanismo.

### Partitioner
O projeto utiliza o `LegacyPartitioner` do KafkaJS para manter compatibilidade com versoes anteriores e evitar warnings.

## Configuracoes Recomendadas para Producao

### Variaveis de Ambiente

Adicione ao seu arquivo `.env`:

```env
NODE_ENV=production
PORT=3000

# Aumentar limites se necessario
# O rate limit esta configurado em index.js (default: 1000 req/min)
```

### Ajustes no ecosystem.config.js

Edite `ecosystem.config.js` conforme suas necessidades:

- **instances**: Numero de workers (default: 'max' = todos os nucleos)
- **max_memory_restart**: Reinicia se usar mais memoria (default: 1GB)
- **max_restarts**: Numero maximo de restarts em caso de erro
- **autorestart**: Reinicio automatico em caso de crash

### Nginx (Reverse Proxy)

Recomendado usar Nginx como reverse proxy:

```nginx
upstream webhook_api {
    least_conn;
    server localhost:3000;
}

server {
    listen 80;
    server_name seu-dominio.com;

    client_max_body_size 2M;

    location / {
        proxy_pass http://webhook_api;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'keep-alive';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
        proxy_connect_timeout 30s;
        proxy_send_timeout 30s;
        proxy_read_timeout 30s;
    }
}
```

### Monitoramento

Configure alertas para:
- Health check retornando 503
- Uso de memoria acima de 80%
- Taxa de erros alta
- Latencia acima de 1s

## Licenca

ISC
