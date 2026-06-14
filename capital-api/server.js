const express = require('express');
const { Pool } = require('pg');
const path = require('path');

const app = express();
const pool = new Pool({
  host: '127.0.0.1',
  port: 5432,
  database: 'trading',
  user: 'trading',
  password: 'Trading2026'
});

// API endpoint
app.get('/api/capital', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        c.trade_no,
        c.trade_id,
        c.capital_after,
        c.pnl_usd,
        c.pnl_pct,
        c.logged_at,
        t.direction,
        t.reason
      FROM capital_log c
      LEFT JOIN trades_a t ON t.id = c.trade_id
      WHERE c.strategie = 'A'
      ORDER BY c.trade_no ASC
    `);
    
    const capital_current = await pool.query(
      `SELECT capital FROM capital_a WHERE id = 1`
    );

    res.json({
      trades: result.rows,
      capital_current: parseFloat(capital_current.rows[0]?.capital || 0),
      updated_at: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Servir la page HTML
app.get('/capital', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(3001, 'localhost', () => {
  console.log('Capital API running on port 3001');
});
