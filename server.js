const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fetch = require('node-fetch');
const cors = require('cors');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());

// Servește fișierele statice din folderul public
app.use(express.static(path.join(__dirname, 'public')));

// Ruta principală - servește index.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// WebSocket pentru funcțiile real-time
wss.on('connection', (ws) => {
    console.log('🔌 Client conectat');
    
    let sessionData = {
        token: null,
        user: null,
        heartbeatInterval: null
    };
    
    ws.on('message', async (data) => {
        try {
            const message = JSON.parse(data);
            
            switch(message.type) {
                case 'login':
                    const response = await fetch('https://discord.com/api/v10/users/@me', {
                        headers: { 'Authorization': message.token }
                    });
                    
                    if (response.ok) {
                        const user = await response.json();
                        sessionData.token = message.token;
                        sessionData.user = user;
                        
                        ws.send(JSON.stringify({
                            type: 'login_success',
                            user: user
                        }));
                        
                        // Pornește heartbeat
                        sessionData.heartbeatInterval = setInterval(async () => {
                            const hb = await fetch('https://discord.com/api/v10/users/@me', {
                                headers: { 'Authorization': sessionData.token }
                            });
                            
                            if (!hb.ok) {
                                clearInterval(sessionData.heartbeatInterval);
                                ws.send(JSON.stringify({
                                    type: 'session_expired'
                                }));
                            }
                        }, 120000);
                        
                    } else {
                        ws.send(JSON.stringify({
                            type: 'login_error',
                            message: 'Token invalid'
                        }));
                    }
                    break;
                    
                case 'logout':
                    if (sessionData.heartbeatInterval) {
                        clearInterval(sessionData.heartbeatInterval);
                    }
                    sessionData.token = null;
                    sessionData.user = null;
                    ws.send(JSON.stringify({ type: 'logged_out' }));
                    break;
            }
        } catch (error) {
            console.error('Eroare:', error);
        }
    });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
    console.log(`🚀 Server pornit pe portul ${PORT}`);
});
