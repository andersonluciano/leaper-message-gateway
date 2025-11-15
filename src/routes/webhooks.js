const express = require('express');
const router = express.Router();

// Rota do webhook para Evolution API
router.post('/evolution-messages/:uuid', async (req, res) => {
  try {
    const { uuid } = req.params;
    const messageData = req.body;

    // Adiciona o companyId (UUID) ao body
    const enrichedData = {
      ...messageData,
      companyId: uuid,
    };

    // Envia a mensagem para o Redpanda
    const producer = req.app.get('producer');
    await producer.sendMessage('whatsapp.evolution_messages', enrichedData);

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
