const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const EventHubReader = require('./scripts/event-hub-reader.js');

// Configuración de las variables de entorno
const iotHubConnectionString = process.env.IotHubConnectionString || 'HostName=WilmerValeria.azure-devices.net;SharedAccessKeyName=service;SharedAccessKey=8Ll3qNz0m2W7YUbGilYRIUdfeWYXaCV8lAIoTAfZ5M8=';
const eventHubConsumerGroup = process.env.EventHubConsumerGroup || 'GRUPO_CONSUMO_WEB_WV';

console.log(`Using IoT Hub connection string [${iotHubConnectionString}]`);
console.log(`Using event hub consumer group [${eventHubConsumerGroup}]`);

// Configuración de la aplicación Express
const app = express();

// Middleware para servir archivos estáticos desde la carpeta 'public'
app.use(express.static(path.join(__dirname, 'public')));

// Middleware para redirigir todas las solicitudes a la raíz '/'
app.use((req, res /* , next */) => {
  res.redirect('/');
});

// Creación del servidor HTTP
const server = http.createServer(app);

// Configuración del WebSocket
const wss = new WebSocket.Server({ server });

// Función para broadcast de mensajes a todos los clientes conectados
wss.broadcast = (data) => {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        console.log(`Broadcasting data ${data}`);
        client.send(data);
      } catch (e) {
        console.error(e);
      }
    }
  });
};

// Iniciar el servidor en el puerto especificado por la variable de entorno PORT, o el puerto 3000 por defecto
server.listen(process.env.PORT || '3000', () => {
  console.log('Listening on %d.', server.address().port);
});

// Iniciar la lectura de mensajes desde el Event Hub
const eventHubReader = new EventHubReader(iotHubConnectionString, eventHubConsumerGroup);

(async () => {
  await eventHubReader.startReadMessage((message, date, deviceId) => {
    try {
      const payload = {
        IotData: message,
        MessageDate: date || new Date().toISOString(),
        DeviceId: deviceId,
      };

      // Transmitir el payload a todos los clientes WebSocket conectados
      wss.broadcast(JSON.stringify(payload));
    } catch (err) {
      console.error('Error broadcasting: [%s] from [%s].', err, message);
    }
  });
})().catch(console.error);
