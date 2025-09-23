const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');

// Import MCP logic from compiled TypeScript
const { initMCPClient, mcpProcessQuery } = require('./mcp-client-bedrock/build/index.js');

const app = express();
app.use(bodyParser.json());

let mcpReady = false;

// Initialize MCP client once at startup
(async () => {
    try {
        // You may want to make these configurable via env or args
        const serverScriptPath = path.resolve(__dirname, './mcp-dynamo/dist/index.js');
        const inferenceProfileId = process.env.INFERENCE_PROFILE_ID;
        await initMCPClient(serverScriptPath, inferenceProfileId);
        mcpReady = true;
        console.log("MCP client initialized.");
    } catch (err) {
        console.error("Failed to initialize MCP client:", err);
    }
})();

app.post('/query', async (req, res) => {
    if (!mcpReady) {
        return res.status(503).json({ error: 'MCP client not ready.' });
    }
    const { query } = req.body;
    if (!query) {
        return res.status(400).json({ error: 'Query is required.' });
    }
    try {
        const result = await mcpProcessQuery(query, true); // stateless mode
        res.json({ result });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`MCP Endpoint listening on port ${PORT}`);
});
