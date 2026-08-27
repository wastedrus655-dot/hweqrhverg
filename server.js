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
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Stocare sesiuni active
const activeSessions = new Map();

// ══════════════════════════════════════════════════════
//  DISCORD API HELPERS
// ══════════════════════════════════════════════════════
async function discordRequest(endpoint, token, options = {}) {
    try {
        const response = await fetch(`https://discord.com/api/v10${endpoint}`, {
            ...options,
            headers: {
                'Authorization': token,
                'Content-Type': 'application/json',
                ...options.headers
            }
        });
        
        if (response.status === 429) {
            const data = await response.json();
            const retryAfter = data.retry_after * 1000;
            await new Promise(r => setTimeout(r, retryAfter));
            return discordRequest(endpoint, token, options);
        }
        
        return response;
    } catch (error) {
        console.error('Discord API Error:', error);
        return null;
    }
}

async function sendMessage(token, channelId, content) {
    const response = await discordRequest(`/channels/${channelId}/messages`, token, {
        method: 'POST',
        body: JSON.stringify({ content })
    });
    return response && response.ok;
}

async function sendTyping(token, channelId) {
    await discordRequest(`/channels/${channelId}/typing`, token, {
        method: 'POST'
    });
}

async function fetchMessages(token, channelId, limit = 10) {
    const response = await discordRequest(`/channels/${channelId}/messages?limit=${limit}`, token);
    return response && response.ok ? await response.json() : [];
}

async function addReaction(token, channelId, messageId, emoji) {
    const encodedEmoji = encodeURIComponent(emoji);
    await discordRequest(`/channels/${channelId}/messages/${messageId}/reactions/${encodedEmoji}/@me`, token, {
        method: 'PUT'
    });
}

async function fetchUserChannels(token) {
    const response = await discordRequest('/users/@me/channels', token);
    return response && response.ok ? await response.json() : [];
}

// ══════════════════════════════════════════════════════
//  WEBSOCKET HANDLER
// ══════════════════════════════════════════════════════
wss.on('connection', (ws) => {
    console.log('🔌 Client conectat');
    
    let sessionData = {
        token: null,
        user: null,
        heartbeatInterval: null,
        afkInterval: null,
        reactInterval: null,
        chatlogInterval: null,
        beefInterval: null,
        pasteInterval: null,
        lastAfkCheck: Date.now(),
        lastReactCheck: Date.now(),
        lastChatlogCheck: Date.now(),
        monitoredChannels: []
    };
    
    ws.on('message', async (data) => {
        try {
            const message = JSON.parse(data);
            console.log('📩 Mesaj primit:', message.type);
            
            switch(message.type) {
                case 'login':
                    await handleLogin(ws, sessionData, message);
                    break;
                    
                case 'send_message':
                    await handleSendMessage(ws, sessionData, message);
                    break;
                    
                case 'send_typing':
                    await handleSendTyping(ws, sessionData, message);
                    break;
                    
                case 'start_heartbeat':
                    startHeartbeat(ws, sessionData);
                    break;
                    
                case 'stop_heartbeat':
                    stopHeartbeat(sessionData);
                    break;
                    
                case 'start_afk':
                    await startAfk(ws, sessionData, message);
                    break;
                    
                case 'stop_afk':
                    stopAfk(sessionData);
                    break;
                    
                case 'start_react':
                    await startReact(ws, sessionData, message);
                    break;
                    
                case 'stop_react':
                    stopReact(sessionData);
                    break;
                    
                case 'start_chatlog':
                    await startChatlog(ws, sessionData, message);
                    break;
                    
                case 'stop_chatlog':
                    stopChatlog(sessionData);
                    break;
                    
                case 'start_beef':
                    await startBeef(ws, sessionData, message);
                    break;
                    
                case 'stop_beef':
                    stopBeef(sessionData);
                    break;
                    
                case 'start_paste':
                    await startPaste(ws, sessionData, message);
                    break;
                    
                case 'stop_paste':
                    stopPaste(sessionData);
                    break;
                    
                case 'start_countdown':
                    await startCountdown(ws, sessionData, message);
                    break;
                    
                case 'stop_countdown':
                    stopCountdown(sessionData);
                    break;
                    
                case 'logout':
                    handleLogout(ws, sessionData);
                    break;
            }
        } catch (error) {
            console.error('WebSocket message error:', error);
            ws.send(JSON.stringify({
                type: 'error',
                message: error.message
            }));
        }
    });
    
    ws.on('close', () => {
        console.log('🔌 Client deconectat');
        cleanupSession(sessionData);
    });
    
    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
        cleanupSession(sessionData);
    });
});

// ══════════════════════════════════════════════════════
//  HANDLERS
// ══════════════════════════════════════════════════════
async function handleLogin(ws, sessionData, message) {
    const { token } = message;
    
    if (!token) {
        ws.send(JSON.stringify({ type: 'login_error', message: 'Token lipsă' }));
        return;
    }
    
    const response = await discordRequest('/users/@me', token);
    
    if (!response || !response.ok) {
        ws.send(JSON.stringify({ type: 'login_error', message: 'Token invalid' }));
        return;
    }
    
    const user = await response.json();
    sessionData.token = token;
    sessionData.user = user;
    
    activeSessions.set(token, { ws, user, sessionData });
    
    ws.send(JSON.stringify({
        type: 'login_success',
        user: {
            id: user.id,
            username: user.username,
            avatar: user.avatar
        }
    }));
    
    console.log(`✅ Utilizator logat: ${user.username}`);
    
    // Pornește heartbeat automat
    startHeartbeat(ws, sessionData);
}

async function handleSendMessage(ws, sessionData, message) {
    const { channelId, content, typing } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    if (typing) {
        await sendTyping(sessionData.token, channelId);
        await new Promise(r => setTimeout(r, 2000));
    }
    
    const success = await sendMessage(sessionData.token, channelId, content);
    
    ws.send(JSON.stringify({
        type: 'message_sent',
        success,
        channelId,
        timestamp: Date.now()
    }));
}

async function handleSendTyping(ws, sessionData, message) {
    const { channelId } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    await sendTyping(sessionData.token, channelId);
}

function startHeartbeat(ws, sessionData) {
    if (sessionData.heartbeatInterval) {
        clearInterval(sessionData.heartbeatInterval);
    }
    
    sessionData.heartbeatInterval = setInterval(async () => {
        if (!sessionData.token) return;
        
        const response = await discordRequest('/users/@me', sessionData.token);
        
        if (response && response.ok) {
            ws.send(JSON.stringify({
                type: 'heartbeat',
                status: 'ok',
                timestamp: Date.now()
            }));
        } else if (response && response.status === 401) {
            ws.send(JSON.stringify({
                type: 'session_expired',
                message: 'Token invalid sau expirat'
            }));
            handleLogout(ws, sessionData);
        }
    }, 120000);
    
    ws.send(JSON.stringify({ type: 'heartbeat_started' }));
}

function stopHeartbeat(sessionData) {
    if (sessionData.heartbeatInterval) {
        clearInterval(sessionData.heartbeatInterval);
        sessionData.heartbeatInterval = null;
    }
}

async function startAfk(ws, sessionData, message) {
    const { channelId, responseText, delay, typing, checkWords } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    sessionData.lastAfkCheck = Date.now();
    const words = checkWords || ['afk', 'check', 'say', 'type', 'here'];
    
    sessionData.afkInterval = setInterval(async () => {
        try {
            const messages = await fetchMessages(sessionData.token, channelId, 5);
            
            for (const msg of messages) {
                const msgTime = new Date(msg.timestamp).getTime();
                
                if (msgTime > sessionData.lastAfkCheck && msg.author.id !== sessionData.user.id) {
                    const content = (msg.content || '').toLowerCase();
                    
                    if (words.some(w => content.includes(w))) {
                        if (typing) {
                            await sendTyping(sessionData.token, channelId);
                            await new Promise(r => setTimeout(r, Math.min(delay || 1000, 2000)));
                        }
                        
                        await new Promise(r => setTimeout(r, delay || 1000));
                        await sendMessage(sessionData.token, channelId, responseText || 'here');
                        
                        ws.send(JSON.stringify({
                            type: 'afk_response',
                            message: responseText || 'here',
                            channelId,
                            timestamp: Date.now()
                        }));
                    }
                }
            }
            
            sessionData.lastAfkCheck = Date.now();
        } catch (error) {
            console.error('AFK polling error:', error);
        }
    }, 3000);
    
    ws.send(JSON.stringify({ type: 'afk_started', channelId }));
}

function stopAfk(sessionData) {
    if (sessionData.afkInterval) {
        clearInterval(sessionData.afkInterval);
        sessionData.afkInterval = null;
    }
}

async function startReact(ws, sessionData, message) {
    const { emoji, channels } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    sessionData.lastReactCheck = Date.now();
    sessionData.monitoredChannels = channels || [];
    
    sessionData.reactInterval = setInterval(async () => {
        try {
            let channelsToCheck = sessionData.monitoredChannels;
            
            if (channelsToCheck.length === 0) {
                channelsToCheck = await fetchUserChannels(sessionData.token);
                channelsToCheck = channelsToCheck.slice(0, 5).map(c => c.id);
            }
            
            for (const channelId of channelsToCheck) {
                const messages = await fetchMessages(sessionData.token, channelId, 3);
                
                for (const msg of messages) {
                    const msgTime = new Date(msg.timestamp).getTime();
                    
                    if (msgTime > sessionData.lastReactCheck && msg.author.id === sessionData.user.id) {
                        await addReaction(sessionData.token, channelId, msg.id, emoji);
                        
                        ws.send(JSON.stringify({
                            type: 'reaction_added',
                            channelId,
                            messageId: msg.id,
                            emoji
                        }));
                    }
                }
            }
            
            sessionData.lastReactCheck = Date.now();
        } catch (error) {
            console.error('React polling error:', error);
        }
    }, 5000);
    
    ws.send(JSON.stringify({ type: 'react_started', emoji }));
}

function stopReact(sessionData) {
    if (sessionData.reactInterval) {
        clearInterval(sessionData.reactInterval);
        sessionData.reactInterval = null;
    }
}

async function startChatlog(ws, sessionData, message) {
    const { channelId } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    sessionData.lastChatlogCheck = Date.now();
    
    sessionData.chatlogInterval = setInterval(async () => {
        try {
            const messages = await fetchMessages(sessionData.token, channelId, 10);
            
            for (const msg of messages.reverse()) {
                const msgTime = new Date(msg.timestamp).getTime();
                
                if (msgTime > sessionData.lastChatlogCheck) {
                    ws.send(JSON.stringify({
                        type: 'chat_message',
                        message: {
                            id: msg.id,
                            content: msg.content,
                            author: {
                                id: msg.author.id,
                                username: msg.author.username,
                                avatar: msg.author.avatar
                            },
                            timestamp: msg.timestamp,
                            channelId
                        }
                    }));
                }
            }
            
            sessionData.lastChatlogCheck = Date.now();
        } catch (error) {
            console.error('Chatlog polling error:', error);
        }
    }, 3000);
    
    ws.send(JSON.stringify({ type: 'chatlog_started', channelId }));
}

function stopChatlog(sessionData) {
    if (sessionData.chatlogInterval) {
        clearInterval(sessionData.chatlogInterval);
        sessionData.chatlogInterval = null;
    }
}

async function startBeef(ws, sessionData, message) {
    const { channelId, words, delay, wordsPerMessage, typing, random } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    if (!words || words.length === 0) {
        ws.send(JSON.stringify({ type: 'error', message: 'Lista de cuvinte e goală' }));
        return;
    }
    
    let index = 0;
    
    sessionData.beefInterval = setInterval(async () => {
        try {
            let msg = '';
            const count = wordsPerMessage || 1;
            
            for (let i = 0; i < count; i++) {
                const word = random 
                    ? words[Math.floor(Math.random() * words.length)]
                    : words[index % words.length];
                msg += word + ' ';
                if (!random) index++;
            }
            
            if (typing) {
                await sendTyping(sessionData.token, channelId);
                await new Promise(r => setTimeout(r, 1500));
            }
            
            await sendMessage(sessionData.token, channelId, msg.trim());
            
            ws.send(JSON.stringify({
                type: 'beef_sent',
                message: msg.trim(),
                channelId
            }));
        } catch (error) {
            console.error('Beef error:', error);
        }
    }, (delay || 2) * 1000);
    
    ws.send(JSON.stringify({ type: 'beef_started', channelId }));
}

function stopBeef(sessionData) {
    if (sessionData.beefInterval) {
        clearInterval(sessionData.beefInterval);
        sessionData.beefInterval = null;
    }
}

async function startPaste(ws, sessionData, message) {
    const { channelId, messages, interval, random, loop } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    if (!messages || messages.length === 0) {
        ws.send(JSON.stringify({ type: 'error', message: 'Lista de mesaje e goală' }));
        return;
    }
    
    let index = 0;
    
    sessionData.pasteInterval = setInterval(async () => {
        try {
            if (index >= messages.length) {
                if (loop) {
                    index = 0;
                } else {
                    stopPaste(sessionData);
                    ws.send(JSON.stringify({ type: 'paste_stopped' }));
                    return;
                }
            }
            
            const msgIndex = random 
                ? Math.floor(Math.random() * messages.length)
                : index;
            
            await sendMessage(sessionData.token, channelId, messages[msgIndex]);
            
            ws.send(JSON.stringify({
                type: 'paste_sent',
                message: messages[msgIndex],
                channelId
            }));
            
            if (!random) index++;
        } catch (error) {
            console.error('Paste error:', error);
        }
    }, (interval || 5) * 1000);
    
    ws.send(JSON.stringify({ type: 'paste_started', channelId }));
}

function stopPaste(sessionData) {
    if (sessionData.pasteInterval) {
        clearInterval(sessionData.pasteInterval);
        sessionData.pasteInterval = null;
    }
}

async function startCountdown(ws, sessionData, message) {
    const { channelId, start, end, delay } = message;
    
    if (!sessionData.token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nu ești logat' }));
        return;
    }
    
    let current = start;
    sessionData.countdownActive = true;
    
    while (current >= end && sessionData.countdownActive) {
        await sendMessage(sessionData.token, channelId, current.toString());
        
        ws.send(JSON.stringify({
            type: 'countdown_tick',
            number: current
        }));
        
        if (current > end) {
            await new Promise(r => setTimeout(r, (delay || 1) * 1000));
        }
        current--;
    }
    
    ws.send(JSON.stringify({ type: 'countdown_complete' }));
}

function stopCountdown(sessionData) {
    sessionData.countdownActive = false;
}

function handleLogout(ws, sessionData) {
    cleanupSession(sessionData);
    activeSessions.delete(sessionData.token);
    ws.send(JSON.stringify({ type: 'logged_out' }));
}

function cleanupSession(sessionData) {
    stopHeartbeat(sessionData);
    stopAfk(sessionData);
    stopReact(sessionData);
    stopChatlog(sessionData);
    stopBeef(sessionData);
    stopPaste(sessionData);
    stopCountdown(sessionData);
    
    if (sessionData.token) {
        activeSessions.delete(sessionData.token);
    }
}

// ══════════════════════════════════════════════════════
//  HTTP API ROUTES (fallback)
// ══════════════════════════════════════════════════════
app.post('/api/login', async (req, res) => {
    const { token } = req.body;
    
    if (!token) {
        return res.status(400).json({ error: 'Token lipsă' });
    }
    
    const response = await discordRequest('/users/@me', token);
    
    if (!response || !response.ok) {
        return res.status(401).json({ error: 'Token invalid' });
    }
    
    const user = await response.json();
    res.json({ user });
});

app.post('/api/send-message', async (req, res) => {
    const { token, channelId, content } = req.body;
    
    if (!token || !channelId || !content) {
        return res.status(400).json({ error: 'Parametri lipsă' });
    }
    
    const success = await sendMessage(token, channelId, content);
    res.json({ success });
});

// ══════════════════════════════════════════════════════
//  START SERVER
// ══════════════════════════════════════════════════════
const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
    console.log(`🚀 Joyce Cord Server pornit pe portul ${PORT}`);
    console.log(`📱 Accesează: http://localhost:${PORT}`);
    console.log(`💓 Toate funcțiile sunt active`);
    console.log(`✅ WebSocket + HTTP API + Heartbeat`);
});

// Curățare la oprire
process.on('SIGINT', () => {
    console.log('\n👋 Oprire server...');
    activeSessions.forEach((session) => {
        cleanupSession(session.sessionData);
    });
    server.close();
    process.exit(0);
});
