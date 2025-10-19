#!/usr/bin/env python3
"""
Pendle Finance MCP Server
Provides tools for querying Pendle market data, analytics, and insights
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Optional, List
import sqlite3

from mcp.server import Server
from mcp.types import Tool, TextContent
import mcp.server.stdio

# Database helper
def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect("pendle_history.db")
    conn.row_factory = sqlite3.Row
    return conn

# Initialize MCP server
app = Server("pendle-mcp-server")

# ==================== TOOL DEFINITIONS ====================

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List all available tools"""
    return [
        Tool(
            name="get_all_markets",
            description="Get a list of all tracked Pendle markets with their latest data including PT price, SY price, and TVL",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="get_market_details",
            description="Get detailed information about a specific Pendle market by its address",
            inputSchema={
                "type": "object",
                "properties": {
                    "market_id": {
                        "type": "string",
                        "description": "The market address (e.g., 0xa83174f1dd8475378abca9d676dad3ce97409e0a)"
                    }
                },
                "required": ["market_id"]
            }
        ),
        Tool(
            name="get_market_history",
            description="Get historical price and TVL data for a specific market over time",
            inputSchema={
                "type": "object",
                "properties": {
                    "market_id": {
                        "type": "string",
                        "description": "The market address"
                    },
                    "hours": {
                        "type": "number",
                        "description": "Number of hours of history to retrieve (default: 24)",
                        "default": 24
                    }
                },
                "required": ["market_id"]
            }
        ),
        Tool(
            name="get_top_markets",
            description="Get top markets ranked by TVL (Total Value Locked)",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "number",
                        "description": "Number of top markets to return (default: 5)",
                        "default": 5
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_analytics_summary",
            description="Get overall analytics summary including total TVL, number of markets, and average prices",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="compare_markets",
            description="Compare two or more markets side by side",
            inputSchema={
                "type": "object",
                "properties": {
                    "market_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of market addresses to compare"
                    }
                },
                "required": ["market_ids"]
            }
        ),
        Tool(
            name="calculate_price_change",
            description="Calculate price change percentage for a market over a time period",
            inputSchema={
                "type": "object",
                "properties": {
                    "market_id": {
                        "type": "string",
                        "description": "The market address"
                    },
                    "hours": {
                        "type": "number",
                        "description": "Time period in hours (default: 24)",
                        "default": 24
                    }
                },
                "required": ["market_id"]
            }
        )
    ]

# ==================== TOOL IMPLEMENTATIONS ====================

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls"""
    
    try:
        if name == "get_all_markets":
            return await get_all_markets()
        elif name == "get_market_details":
            return await get_market_details(arguments["market_id"])
        elif name == "get_market_history":
            return await get_market_history(
                arguments["market_id"],
                arguments.get("hours", 24)
            )
        elif name == "get_top_markets":
            return await get_top_markets(arguments.get("limit", 5))
        elif name == "get_analytics_summary":
            return await get_analytics_summary()
        elif name == "compare_markets":
            return await compare_markets(arguments["market_ids"])
        elif name == "calculate_price_change":
            return await calculate_price_change(
                arguments["market_id"],
                arguments.get("hours", 24)
            )
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error executing {name}: {str(e)}")]

# ==================== TOOL FUNCTIONS ====================

async def get_all_markets() -> list[TextContent]:
    """Get all markets with latest data"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get latest snapshot for each market
    query = """
        SELECT m1.*
        FROM market_snapshots m1
        INNER JOIN (
            SELECT market_id, MAX(timestamp) as max_ts
            FROM market_snapshots
            GROUP BY market_id
        ) m2 ON m1.market_id = m2.market_id AND m1.timestamp = m2.max_ts
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    markets = []
    for row in rows:
        try:
            raw_data = json.loads(row['raw_json'])
            market_name = raw_data.get('name', 'Unknown')
        except:
            market_name = 'Unknown'
        
        markets.append({
            "market_id": row['market_id'],
            "name": market_name,
            "pt_price": row['pt_price'],
            "sy_price": row['sy_price'],
            "tvl": row['tvl'],
            "last_updated": row['timestamp']
        })
    
    conn.close()
    
    result = {
        "count": len(markets),
        "markets": markets
    }
    
    return [TextContent(
        type="text",
        text=json.dumps(result, indent=2)
    )]

async def get_market_details(market_id: str) -> list[TextContent]:
    """Get detailed info for a specific market"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM market_snapshots 
        WHERE market_id = ? 
        ORDER BY timestamp DESC 
        LIMIT 1
    """, (market_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return [TextContent(type="text", text=f"Market {market_id} not found")]
    
    try:
        full_data = json.loads(row['raw_json'])
    except:
        full_data = {}
    
    result = {
        "market_id": row['market_id'],
        "timestamp": row['timestamp'],
        "pt_price": row['pt_price'],
        "sy_price": row['sy_price'],
        "tvl": row['tvl'],
        "full_data": full_data
    }
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]

async def get_market_history(market_id: str, hours: int = 24) -> list[TextContent]:
    """Get historical data for a market"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
    
    cursor.execute("""
        SELECT timestamp, pt_price, sy_price, tvl
        FROM market_snapshots
        WHERE market_id = ? AND timestamp >= ?
        ORDER BY timestamp ASC
    """, (market_id, cutoff))
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return [TextContent(type="text", text=f"No history found for market {market_id}")]
    
    history = [dict(row) for row in rows]
    
    result = {
        "market_id": market_id,
        "data_points": len(history),
        "history": history
    }
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]

async def get_top_markets(limit: int = 5) -> list[TextContent]:
    """Get top markets by TVL"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT m1.*, json_extract(m1.raw_json, '$.name') as name
        FROM market_snapshots m1
        INNER JOIN (
            SELECT market_id, MAX(timestamp) as max_ts
            FROM market_snapshots
            GROUP BY market_id
        ) m2 ON m1.market_id = m2.market_id AND m1.timestamp = m2.max_ts
        ORDER BY m1.tvl DESC
        LIMIT ?
    """
    
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    top_markets = []
    for row in rows:
        top_markets.append({
            "market_id": row['market_id'],
            "name": row['name'] or 'Unknown',
            "tvl": row['tvl'],
            "pt_price": row['pt_price'],
            "sy_price": row['sy_price']
        })
    
    return [TextContent(type="text", text=json.dumps({"top_markets": top_markets}, indent=2))]

async def get_analytics_summary() -> list[TextContent]:
    """Get overall analytics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Total markets
    cursor.execute("SELECT COUNT(DISTINCT market_id) FROM market_snapshots")
    total_markets = cursor.fetchone()[0]
    
    # Total snapshots
    cursor.execute("SELECT COUNT(*) FROM market_snapshots")
    total_snapshots = cursor.fetchone()[0]
    
    # Get latest snapshots for aggregation
    query = """
        SELECT m1.tvl, m1.pt_price
        FROM market_snapshots m1
        INNER JOIN (
            SELECT market_id, MAX(timestamp) as max_ts
            FROM market_snapshots
            GROUP BY market_id
        ) m2 ON m1.market_id = m2.market_id AND m1.timestamp = m2.max_ts
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    total_tvl = sum(row['tvl'] for row in rows if row['tvl'])
    avg_pt_price = sum(row['pt_price'] for row in rows if row['pt_price']) / len(rows) if rows else 0
    
    result = {
        "total_markets": total_markets,
        "total_tvl": total_tvl,
        "average_pt_price": avg_pt_price,
        "total_snapshots": total_snapshots,
        "timestamp": datetime.now().isoformat()
    }
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]

async def compare_markets(market_ids: List[str]) -> list[TextContent]:
    """Compare multiple markets"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    placeholders = ','.join('?' * len(market_ids))
    query = f"""
        SELECT m1.*, json_extract(m1.raw_json, '$.name') as name
        FROM market_snapshots m1
        INNER JOIN (
            SELECT market_id, MAX(timestamp) as max_ts
            FROM market_snapshots
            WHERE market_id IN ({placeholders})
            GROUP BY market_id
        ) m2 ON m1.market_id = m2.market_id AND m1.timestamp = m2.max_ts
    """
    
    cursor.execute(query, market_ids)
    rows = cursor.fetchall()
    conn.close()
    
    comparison = []
    for row in rows:
        comparison.append({
            "market_id": row['market_id'],
            "name": row['name'] or 'Unknown',
            "pt_price": row['pt_price'],
            "sy_price": row['sy_price'],
            "tvl": row['tvl'],
            "timestamp": row['timestamp']
        })
    
    return [TextContent(type="text", text=json.dumps({"comparison": comparison}, indent=2))]

async def calculate_price_change(market_id: str, hours: int = 24) -> list[TextContent]:
    """Calculate price change over time"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
    
    cursor.execute("""
        SELECT pt_price, sy_price, timestamp
        FROM market_snapshots
        WHERE market_id = ? AND timestamp >= ?
        ORDER BY timestamp ASC
    """, (market_id, cutoff))
    
    rows = cursor.fetchall()
    conn.close()
    
    if len(rows) < 2:
        return [TextContent(type="text", text=f"Insufficient data to calculate price change for {market_id}")]
    
    first = rows[0]
    last = rows[-1]
    
    pt_change = ((last['pt_price'] - first['pt_price']) / first['pt_price'] * 100) if first['pt_price'] else 0
    sy_change = ((last['sy_price'] - first['sy_price']) / first['sy_price'] * 100) if first['sy_price'] else 0
    
    result = {
        "market_id": market_id,
        "time_period_hours": hours,
        "pt_price_change_percent": round(pt_change, 4),
        "sy_price_change_percent": round(sy_change, 4),
        "start_time": first['timestamp'],
        "end_time": last['timestamp'],
        "start_pt_price": first['pt_price'],
        "end_pt_price": last['pt_price']
    }
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]

# ==================== MAIN ====================

async def main():
    """Run the MCP server"""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
