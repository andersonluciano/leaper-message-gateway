module.exports = {
  apps: [
    {
      name: 'webhook-messages-api',
      script: './index.js',

      // Modo cluster para aproveitar todos os nucleos da CPU
      instances: 'max', // ou numero especifico como 4
      exec_mode: 'cluster',

      // Variaveis de ambiente
      env: {
        NODE_ENV: 'development',
        PORT: 3000,
      },
      env_production: {
        NODE_ENV: 'production',
        PORT: 3000,
      },

      // Auto restart em caso de falha
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',

      // Logs
      error_file: './logs/error.log',
      out_file: './logs/out.log',
      log_file: './logs/combined.log',
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',

      // Graceful shutdown
      kill_timeout: 5000,
      wait_ready: true,
      listen_timeout: 10000,

      // Estrategia de restart
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,

      // Merge logs de todos os clusters
      merge_logs: true,

      // Configuracoes de performance
      instance_var: 'INSTANCE_ID',
    },
  ],
};
