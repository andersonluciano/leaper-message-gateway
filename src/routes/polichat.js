const express = require('express');
const crypto = require('crypto');
const router = express.Router();

/**
 * Middleware de autenticação por API Key
 * Valida a chave recebida via queryString usando SHA256
 */
function authenticateApiKey(req, res, next) {
    try {
        const apiKey = req.query.apiKey;

        if (!apiKey) {
            return res.status(401).json({
                success: false,
                message: 'API Key nao fornecida',
            });
        }

        // Gera o hash SHA256 da chave recebida
        const receivedKeyHash = crypto
            .createHash('sha256')
            .update(apiKey)
            .digest('hex');

        // Compara com o hash armazenado no .env
        const storedKeyHash = process.env.POLICHAT_API_KEY_HASH;

        if (!storedKeyHash) {
            console.error('[ERRO] POLICHAT_API_KEY_HASH nao configurado no .env');
            return res.status(500).json({
                success: false,
                message: 'Configuracao de autenticacao invalida',
            });
        }

        if (receivedKeyHash !== storedKeyHash) {
            return res.status(401).json({
                success: false,
                message: 'API Key invalida',
            });
        }

        // Autenticação bem-sucedida
        next();
    } catch (error) {
        console.error('Erro ao validar API Key:', error);
        res.status(500).json({
            success: false,
            message: 'Erro ao validar autenticacao',
            error: error.message,
        });
    }
}

// Rota do webhook para Polichat
router.post('/polichat-messages/:uuid', authenticateApiKey, async (req, res) => {
    try {
        const {uuid} = req.params;
        const messageData = req.body;

        // Adiciona o companyId (UUID) ao body
        const enrichedData = {
            ...messageData,
            companyId: uuid,
        };

        // Envia a mensagem para o Redpanda no tópico polichat
        const producer = req.app.get('producer');
        await producer.sendMessage('whatsapp.polichat_messages', enrichedData);

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

module.exports = router;
