from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import asyncio
import aiohttp
import websockets
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING
import logging
from contextlib import asynccontextmanager
import itertools
import time
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB configuration
MONGODB_URL = "mongodb://localhost:27017"
DATABASE_NAME = "coinbase_data"
COLLECTION_NAME = "market_data"

# Coinbase API URLs
COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"
COINBASE_STATS_URL = "https://api.exchange.coinbase.com/products/{}/stats"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

# COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# class PriceRequest(BaseModel):
#     coins: List[str] = []
#     vs_currency: str = "usd"
#     all: bool = False  # 

class CoinbaseDataManager:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URL)
        self.db = self.client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]
        self.websocket = None
        self.running = False

    # async def get_all_coin_ids(self, vs_currency: str = "usd", max_pages: int = 10, delay: float = 1.5) -> List[str]:
    #     """Fetch all active coin IDs with market data, with rate limiting"""
    #     async with httpx.AsyncClient() as client:
    #         try:
    #             ids = []
    #             page = 1
    #             while page <= max_pages:
    #                 url = (
    #                     f"{COINGECKO_BASE_URL}/coins/markets"
    #                     f"?vs_currency={vs_currency}&order=market_cap_desc&per_page=250&page={page}"
    #                 )
    #                 response = await client.get(url)
    #                 response.raise_for_status()
    #                 data = response.json()
    #                 if not data:
    #                     break
    #                 ids.extend([coin["id"] for coin in data])
    #                 page += 1
    #                 await asyncio.sleep(delay)  # Respect rate limits
    #             return ids
    #         except httpx.RequestError as e:
    #             raise HTTPException(status_code=500, detail=f"Error fetching market coins: {str(e)}")

    # async def get_all_coin_prices(self, vs_currency: str = "usd", chunk_size: int = 250) -> Dict[str, Any]:
    #     """Fetch prices for all known coins, in chunks"""
    #     all_ids = await self.get_all_coin_ids()
    #     chunks = self.chunk_list(all_ids, chunk_size)

    #     tasks = [self.get_prices_for_chunk(chunk, vs_currency) for chunk in chunks]
    #     results = await asyncio.gather(*tasks)

    #     all_prices = {}
    #     for result in results:
    #         all_prices.update(result)
    #     return all_prices

    # async def get_all_coin_ids(self) -> List[str]:
    #     """Fetch all known coin IDs from CoinGecko"""
    #     async with httpx.AsyncClient() as client:
    #         try:
    #             url = f"{COINGECKO_BASE_URL}/coins/list"
    #             response = await client.get(url)
    #             response.raise_for_status()
    #             coins = response.json()
    #             return [coin["id"] for coin in coins]
    #         except httpx.RequestError as e:
    #             raise HTTPException(status_code=500, detail=f"Error fetching coin list: {str(e)}")


    # def chunk_list(self, lst: List[str], chunk_size: int) -> List[List[str]]:
    #     """Split a list into smaller chunks"""
    #     return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


    # async def get_prices_for_chunk(self, coin_ids: List[str], vs_currency: str = "usd") -> Dict[str, Any]:
    #     """Fetch prices for a chunk of coin IDs"""
    #     ids_str = ','.join(coin_ids)
    #     url = f"{COINGECKO_BASE_URL}/simple/price?ids={ids_str}&vs_currencies={vs_currency}&include_24hr_change=true&include_market_cap=true"
    #     async with httpx.AsyncClient() as client:
    #         try:
    #             response = await client.get(url)
    #             response.raise_for_status()
    #             return response.json()
    #         except httpx.RequestError as e:
    #             raise HTTPException(status_code=500, detail=f"Error fetching prices: {str(e)}")

    async def fetch_candles(product_id: str, start: datetime, end: datetime, granularity: int = 60) -> List[dict]:
        MAX_CANDLES = 300
        max_range_seconds = granularity * MAX_CANDLES

        headers = {"Accept": "application/json"}
        all_candles = []

        async with aiohttp.ClientSession() as session:
            current_start = start

            while current_start < end:
                current_end = min(current_start + timedelta(seconds=max_range_seconds), end)

                params = {
                    "start": current_start.isoformat(),
                    "end": current_end.isoformat(),
                    "granularity": granularity
                }

                url = f"{COINBASE_PRODUCTS_URL}/{product_id}/candles"

                async with session.get(url, params=params, headers=headers) as resp:
                    data = await resp.json()

                    if isinstance(data, list):
                        all_candles.extend(data)
                    else:
                        print(f"Error fetching {product_id}:", data)
                        break  # Stop on API error to avoid spamming

                current_start = current_end  # Move to next chunk

        sorted_candles = sorted(all_candles, key=lambda c: c[0])  # sort by time
        return [{"timestamp": datetime.utcfromtimestamp(c[0]), "close": c[4]} for c in sorted_candles]
            
    async def fetch_product_ids() -> List[str]:
        async with aiohttp.ClientSession() as session:
            async with session.get(COINBASE_PRODUCTS_URL) as resp:
                resp.raise_for_status()  # Raise error for HTTP failure
                data = await resp.json()
                return [p["id"] for p in data if "id" in p]

    def compute_correlations(price_map: Dict[str, List[float]]) -> List[Dict]:
        results = []
        for a, b in itertools.combinations(price_map.keys(), 2):
            prices_a = np.array(price_map[a])
            prices_b = np.array(price_map[b])
            min_len = min(len(prices_a), len(prices_b))
            if min_len > 0:
                corr = np.corrcoef(prices_a[:min_len], prices_b[:min_len])[0, 1]
                results.append({
                    "asset": a,
                    "correlated_with": b,
                    "correlation": round(corr, 6)
                })
        # Sort by absolute correlation and limit to top 10
        return sorted(results, key=lambda x: abs(x["correlation"]), reverse=True)[:10]

    async def initialize_db(self):
        """Initialize database with indexes"""
        try:
            # Create indexes for better query performance
            await self.collection.create_index([("product_id", ASCENDING), ("timestamp", DESCENDING)])
            await self.collection.create_index([("timestamp", DESCENDING)])
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
    
    async def get_products(self) -> List[Dict]:
        """Fetch the top 10 available correlations from Coinbase, excluding certain fields"""
        excluded_keys = {
            "margin_enabled",
            "post_only",
            "limit_only",
            "cancel_only",
            "status_message",
            "trading_disabled",
            "fx_stablecoin",
            "auction_mode",
            "high_bid_limit_percentage",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(COINBASE_PRODUCTS_URL) as response:
                    if response.status == 200:
                        products = await response.json()
                        top_10 = products[:10]

                        # Remove excluded keys from each product dict
                        filtered_products = [
                            {k: v for k, v in product.items() if k not in excluded_keys}
                            for product in top_10
                        ]
                        return filtered_products
                    else:
                        logger.error(f"Failed to fetch products: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Error fetching correlations: {e}")
            return []
    
    async def get_product_stats(self, product_id: str) -> Dict:
        """Fetch 24hr stats for a specific correlations"""
        try:
            async with aiohttp.ClientSession() as session:
                url = COINBASE_STATS_URL.format(product_id)
                async with session.get(url) as response:
                    if response.status == 200:
                        stats = await response.json()
                        stats['product_id'] = product_id
                        stats['timestamp'] = datetime.utcnow()
                        return stats
                    else:
                        logger.error(f"Failed to fetch stats for {product_id}: {response.status}")
                        return {}
        except Exception as e:
            logger.error(f"Error fetching stats for {product_id}: {e}")
            return {}
    
    async def get_all_product_ids(self) -> List[str]:
        product_ids = await self.collection.distinct("product_id")
        return product_ids

    # async def get_top_correlated_product_details(self, top_correlated_ids: List[str]) -> List[Dict]:
    #     """Return full product details for top correlated product IDs"""
    #     try:
    #         all_products = await self.get_products()
    #         # Create a dictionary for quick lookup by product ID
    #         product_dict = {product['id']: product for product in all_products}
            
    #         detailed_products = []
    #         for product_id in top_correlated_ids:
    #             product = product_dict.get(product_id)
    #             if product:
    #                 detailed_products.append(product)
    #             else:
    #                 logger.warning(f"Product ID {product_id} not found in Coinbase product list.")

    #         return detailed_products
    #     except Exception as e:
    #         logger.error(f"Error retrieving detailed product info: {e}")
    #         return []
    
    async def get_top_products_by_volume(self, limit: int = 50) -> List[str]:
        """Get top correlations by trading volume"""
        products = await self.get_products()
        if not products:
            return []
        
        active_products = [
            p for p in products 
            if p.get('status') == 'online'
        ]
        
        # Get stats for each product (increased limit for top 50)
        product_stats = []
        
        # Process products in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, min(len(active_products), 100), batch_size):  # Process up to 100 products
            batch = active_products[i:i + batch_size]
            
            # Create tasks for concurrent API calls
            tasks = [self.get_product_stats(product['id']) for product in batch]
            batch_stats = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, stats in enumerate(batch_stats):
                if isinstance(stats, dict) and stats and 'volume' in stats:
                    try:
                        volume = float(stats['volume'])
                        if volume > 0:  # Only include products with actual volume
                            product_stats.append({
                                'product_id': batch[j]['id'],
                                'volume': volume
                            })
                    except (ValueError, TypeError):
                        continue
            
            # Small delay between batches to be respectful to API
            await asyncio.sleep(0.1)
        
        # Sort by volume and return top products
        product_stats.sort(key=lambda x: x['volume'], reverse=True)
        return [p['product_id'] for p in product_stats[:limit]]
    
    async def store_market_data(self, data: Dict):
        """Store market data in MongoDB"""
        try:
            data['timestamp'] = datetime.utcnow()
            await self.collection.insert_one(data)
        except Exception as e:
            logger.error(f"Error storing market data: {e}")
    
    # async def get_historical_data(self, product_ids: List[str], hours: int = 24) -> pd.DataFrame:
    #     """Retrieve historical data for correlation analysis"""
    #     try:
    #         start_time = datetime.utcnow() - timedelta(hours=hours)
            
    #         cursor = self.collection.find({
    #             'product_id': {'$in': product_ids},
    #             'timestamp': {'$gte': start_time}
    #         }).sort('timestamp', 1)
            
    #         data = await cursor.to_list(length=None)
            
    #         if not data:
    #             return pd.DataFrame()
            
    #         df = pd.DataFrame(data)
    #         return df
    #     except Exception as e:
    #         logger.error(f"Error retrieving historical data: {e}")
    #         return pd.DataFrame()
    
    # async def calculate_correlation_matrix(self, product_ids: List[str]) -> Dict:
    #     """Calculate correlation matrix for top products"""
    #     try:
    #         df = await self.get_historical_data(product_ids)
            
    #         if df.empty:
    #             return {"error": "No historical data available"}
            
    #         # Create pivot table with prices
    #         price_data = df.pivot_table(
    #             index='timestamp', 
    #             columns='product_id', 
    #             values='price',
    #             fill_method='ffill'
    #         )
            
    #         if price_data.empty:
    #             return {"error": "Insufficient price data for correlation analysis"}
            
    #         # Calculate returns
    #         returns = price_data.pct_change().dropna()
            
    #         if returns.empty:
    #             return {"error": "Insufficient data to calculate returns"}
            
    #         # Calculate correlation matrix
    #         correlation_matrix = returns.corr()
            
    #         # Convert to serializable format
    #         result = {
    #             'correlation_matrix': correlation_matrix.round(4).to_dict(),
    #             'products': list(correlation_matrix.columns),
    #             'timestamp': datetime.utcnow().isoformat(),
    #             'data_points': len(returns)
    #         }
            
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"Error calculating correlation matrix: {e}")
    #         return {"error": f"Correlation calculation failed: {str(e)}"}
    
    # async def get_high_frequency_data(self, product_ids: List[str], minutes: int = 60) -> pd.DataFrame:
    #     """
    #     Retrieve high-frequency tick data from MongoDB with full millisecond precision.
    #     Supports sub-second correlation analysis (e.g., 100ms granularity).
    #     """
    #     try:
    #         start_time = datetime.utcnow() - timedelta(minutes=minutes)
    #         cursor = self.collection.find(
    #             {
    #                 'product_id': {'$in': product_ids},
    #                 'timestamp': {'$gte': start_time}
    #             },
    #             projection={
    #                 '_id': 0,
    #                 'product_id': 1,
    #                 'timestamp': 1,
    #                 'price': 1,
    #                 'volume_24h': 1  # optional
    #             }
    #         ).sort('timestamp', 1)

    #         data = await cursor.to_list(length=None)
    #         if not data:
    #             return pd.DataFrame()

    #         df = pd.DataFrame(data)
            
    #         # Ensure timestamp is parsed correctly and has timezone
    #         df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    #         return df

    #     except Exception as e:
    #         import traceback
    #         print(f"Error fetching high-frequency data: {e}")
    #         print(traceback.format_exc())
    #         return pd.DataFrame()
            

    async def calculate_top_correlated_pairs(
            self,
            minutes: int = 60,
            granularity_seconds: float = 0.1
        ) -> Dict:
        try:
            from datetime import datetime, timedelta
            import numpy as np
            import pandas as pd

            start_time = datetime.utcnow() - timedelta(minutes=minutes)
            granularity_ms = int(granularity_seconds * 1000)
            granularity_str = f"{granularity_ms}ms"

            # 🔍 Query all data since start_time (no product_id filter)
            cursor = self.collection.find(
                {
                    "timestamp": {"$gte": start_time}
                },
                {
                    "_id": 0,
                    "product_id": 1,
                    "timestamp": 1,
                    "price": 1
                }
            ).sort("timestamp", 1)

            data = await cursor.to_list(length=None)
            if not data:
                return {"error": "No price data available for the specified time window"}

            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

            # Fix: Aggregate duplicate timestamp/product_id entries by taking the last price
            df_agg = df.groupby(["timestamp", "product_id"])["price"].last().reset_index()

            price_data = df_agg.pivot(index="timestamp", columns="product_id", values="price").sort_index()

            start_time = price_data.index.min().floor(f"{granularity_ms}ms")
            end_time = price_data.index.max().floor(f"{granularity_ms}ms")
            full_index = pd.date_range(start=start_time, end=end_time, freq=granularity_str)

            price_data_resampled = price_data.resample(granularity_str).last()
            price_data = price_data_resampled.reindex(full_index).ffill().dropna(axis=1, how="all")

            if price_data.empty or len(price_data.columns) < 2:
                return {"error": "Insufficient price data for correlation analysis"}

            returns = price_data.pct_change().dropna()
            min_data_points = max(30, int(30 / granularity_seconds))
            if returns.empty or len(returns) < min_data_points:
                return {
                    "error": f"Insufficient data points for reliable correlation (need at least {min_data_points} data points)"
                }

            correlation_matrix = returns.corr()
            high_correlations = []
            n = len(correlation_matrix.columns)
            for i in range(n):
                for j in range(i + 1, n):
                    corr_val = correlation_matrix.iloc[i, j]
                    if not pd.isna(corr_val):
                        high_correlations.append({
                            "asset": correlation_matrix.columns[i],
                            "correlated_with": correlation_matrix.columns[j],
                            "correlation": round(corr_val, 6)
                        })
                        high_correlations.sort(key=lambda x: abs(x["correlation"]), reverse=True)

            # Top 10 highest absolute correlation pairs
            top_10_pairs = high_correlations[:10]

            return {
                "top_correlated_pairs": top_10_pairs,
                "total_pairs": len(high_correlations),
                "time_span_minutes": minutes,
                "granularity_seconds": granularity_seconds,
                "granularity_ms": granularity_ms,
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            import traceback
            return {
                "error": f"Correlation analysis failed: {str(e)}",
                "traceback": traceback.format_exc()
            }


        
    async def start_websocket_feed(self, product_ids: List[str]):
        """Start WebSocket feed for real-time data"""
        if self.running:
            return
        
        self.running = True
        
        subscribe_message = {
            "type": "subscribe",
            "product_ids": product_ids,
            "channels": ["ticker"]
        }
        
        try:
            async with websockets.connect(COINBASE_WS_URL) as websocket:
                self.websocket = websocket
                await websocket.send(json.dumps(subscribe_message))
                logger.info(f"Subscribed to WebSocket feed for {len(product_ids)} products")
                
                while self.running:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                        data = json.loads(message)
                        
                        if data.get('type') == 'ticker':
                            ticker_data = {
                                'product_id': data.get('product_id'),
                                'price': float(data.get('price', 0)),
                                'volume_24h': float(data.get('volume_24h', 0)),
                                'best_bid': float(data.get('best_bid', 0)),
                                'best_ask': float(data.get('best_ask', 0)),
                                'high_24h': float(data.get('high_24h', 0)),
                                'low_24h': float(data.get('low_24h', 0)),
                            }
                            await self.store_market_data(ticker_data)
                            
                    except asyncio.TimeoutError:
                        logger.warning("WebSocket timeout, sending ping")
                        await websocket.ping()
                    except Exception as e:
                        logger.error(f"WebSocket error: {e}")
                        break
                        
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
        finally:
            self.running = False
            self.websocket = None
    
    async def stop_websocket_feed(self):
        """Stop WebSocket feed"""
        self.running = False
        if self.websocket:
            await self.websocket.close()

# Global data manager instance
data_manager = CoinbaseDataManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await data_manager.initialize_db()
    logger.info("Application started")
    yield
    # Shutdown
    await data_manager.stop_websocket_feed()
    logger.info("Application shutdown")

# Initialize FastAPI app
app = FastAPI(
    title="Correlation Analysis",
    description="Real-time market data collection and correlation analysis for top cryptocurrencies",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """API health check"""
    return {
        "message": "Correlation Analysis",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat()
    }

# @app.get("/top_10/correlations")
# async def get_top_10_correlations():
#     """Get top 10 correlations"""
#     correlations = await data_manager.get_products()
#     return {"correlations": correlations, "count": len(correlations)}

# @app.post("/crypto/prices")
# async def get_all_crypto_prices() -> Dict[str, Any]:
#     """Get current prices for all known cryptocurrencies"""
#     return await data_manager.get_all_coin_ids(vs_currency="usd")

@app.get("/daily/correlation-analysis")
async def get_daily_correlation_analysis(
    minutes: int = Query(100, ge=1, le=1440, description="Time window in minutes (1-1440)"),
    granularity: float = Query(1.0, ge=0.001, le=60.0, description="Granularity in seconds (0.001-60.0)")
):
    """
    Get high-frequency correlation analysis for all available correlations,
    returning the top 10 most correlated pairs with configurable granularity.
    
    Parameters:
    - minutes: Time window in minutes (1-1440, max 24 hours)
    - granularity: Time granularity in seconds (0.001-60.0)
      - 1.0 = 1 second intervals
      - 0.1 = 100ms intervals  
      - 0.01 = 10ms intervals
      - 0.001 = 1ms intervals
    """
    
    max_data_points = minutes * 60 / granularity
    if max_data_points > 100000:  # Limit to prevent performance issues
        raise HTTPException(
            status_code=400,
            detail=f"Combination of minutes ({minutes}) and granularity ({granularity}s) would generate {max_data_points:,.0f} data points. Maximum allowed is 100,000. Please reduce time window or increase granularity."
        )
    
    if granularity <= 0.01 and minutes > 10:
        raise HTTPException(
            status_code=400,
            detail=f"For granularity ≤ 0.01s (10ms), maximum time window is 10 minutes. Current: {minutes} minutes."
        )
    elif granularity <= 0.1 and minutes > 60:
        raise HTTPException(
            status_code=400,
            detail=f"For granularity ≤ 0.1s (100ms), maximum time window is 60 minutes. Current: {minutes} minutes."
        )
    
    try:
        # Get all product IDs available in the DB
        all_products = await data_manager.get_all_product_ids()
        if not all_products or len(all_products) < 2:
            raise HTTPException(status_code=404, detail="Not enough correlations available for correlation analysis")
        
        # Run correlation on all products using sub-second analysis
        correlation_data = await data_manager.calculate_top_correlated_pairs(minutes, granularity)
        if "error" in correlation_data:
            if "Insufficient data" in correlation_data["error"]:
                raise HTTPException(status_code=404, detail=correlation_data["error"])
            elif "No high-frequency data" in correlation_data["error"]:
                raise HTTPException(status_code=404, detail="No price data available for the specified time window")
            else:
                raise HTTPException(status_code=500, detail=f"Correlation analysis failed: {correlation_data['error']}")
        
        # Extract top 10 correlations by absolute correlation value
        top_correlations = sorted(
            correlation_data.get("top_correlated_pairs", []),
            key=lambda x: abs(x["correlation"]),
            reverse=True
        )[:10]

        correlation_data["api_metadata"] = {
            "requested_params": {
                "minutes": minutes,
                "granularity_seconds": granularity,
                "total_correlation_analyzed": len(all_products)
            },
            "data_points_requested": int(minutes * 60 / granularity),
            "top_correlation_pairs_returned": len(top_correlations),
            "analysis_type": "sub_second" if granularity < 1.0 else "standard"
        }
        
        return correlation_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during correlation analysis: {str(e)}"
        )

@app.get("/current-week/correlation-analysis")
async def get_current_week_correlations(max_correlation: int = 10):
    now = datetime.utcnow()
    start_of_week = now - timedelta(days=now.weekday())  # Monday 00:00 UTC
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    granularity = 60 

    product_ids = await CoinbaseDataManager.fetch_product_ids()
    product_ids = product_ids[:max_correlation]

    candles_list = await asyncio.gather(
        *[CoinbaseDataManager.fetch_candles(pid, start_of_week, now, granularity) for pid in product_ids]
    )

    price_map = {
        pid: [c["close"] for c in candles]
        for pid, candles in zip(product_ids, candles_list)
        if len(candles) > 200
    }

    print(f"Correlations for this week with enough candle data: {len(price_map)}")

    correlations = CoinbaseDataManager.compute_correlations(price_map)

    return JSONResponse(content={
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "start": start_of_week.isoformat() + "Z",
        "end": now.isoformat() + "Z",
        "product_count": len(price_map),
        "correlations": correlations
    })
    
@app.get("/last-week/correlation-analysis")
async def get_last_week_correlations(max_correlation: int = 10):
    end = datetime.utcnow()
    start = end - timedelta(days=7)
    granularity = 60 

    product_ids = await CoinbaseDataManager.fetch_product_ids()
    product_ids = product_ids[:max_correlation]

    candles_list = await asyncio.gather(
        *[CoinbaseDataManager.fetch_candles(pid, start, end, granularity) for pid in product_ids]
    )

    price_map = {
        pid: [c["close"] for c in candles]
        for pid, candles in zip(product_ids, candles_list)
        if len(candles) > 200
    }

    correlations = CoinbaseDataManager.compute_correlations(price_map)

    return JSONResponse(content={
        "generated_at": datetime.utcnow().isoformat() + "Z",  # precise time of generation
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z",
        "product_count": len(price_map),
        "correlations": correlations
    })

# @app.get("/top-correlation/{limit}")
# async def get_top_correlations(limit: int = 10):
#     """Get top products by trading volume"""
#     if limit > 50:
#         raise HTTPException(status_code=400, detail="Limit cannot exceed 50")
    
#     products = await data_manager.get_top_correlated_product_details(limit)
#     return {"top_products": products, "count": len(products)}

# @app.get("/stats/{product_id}")
# async def get_correlatio_stats(product_id: str):
#     """Get 24hr stats for a specific product"""
#     stats = await data_manager.get_product_stats(product_id)
#     if not stats:
#         raise HTTPException(status_code=404, detail="Product not found or no data available")
#     return stats

@app.post("/start-feed")
async def start_data_feed(background_tasks: BackgroundTasks, limit: int = 10):
    """Start real-time data collection for top correlations"""
    if data_manager.running:
        return {"message": "Data feed is already running"}
    
    top_products = await data_manager.get_top_products_by_volume(limit)
    if not top_products:
        raise HTTPException(status_code=404, detail="No correlations found")
    
    background_tasks.add_task(data_manager.start_websocket_feed, top_products)
    
    return {
        "message": "Real-time data feed started",
        "correlations": top_products,
        "count": len(top_products)
    }

@app.post("/stop-feed")
async def stop_data_feed():
    """Stop real-time data collection"""
    await data_manager.stop_websocket_feed()
    return {"message": "Data feed stopped"}

@app.get("/feed-status")
async def get_feed_status():
    """Get current status of data feed"""
    return {
        "running": data_manager.running,
        "timestamp": datetime.utcnow().isoformat()
    }

# @app.get("/candles")
# async def get_candles(
#     product_ids: List[str] = Query(..., description="List of product IDs like BTC-USD, ETH-USD"),
#     start: datetime = Query(..., description="Start time in ISO 8601"),
#     end: datetime = Query(..., description="End time in ISO 8601"),
#     granularity: int = Query(3600, description="Candle granularity in seconds")
# ):
#     results = await asyncio.gather(
#         *[CoinbaseDataManager.fetch_candles(pid, start, end, granularity) for pid in product_ids]
#     )
#     return {pid: candles for pid, candles in zip(product_ids, results)}



# @app.get("/historical-data/{product_id}")
# async def get_historical_data(product_id: str, hours: int = 24):
#     """Get historical data for a specific product"""
#     if hours > 168:
#         raise HTTPException(status_code=400, detail="Hours cannot exceed 168 (1 week)")
    
#     df = await data_manager.get_historical_data([product_id], hours)
    
#     if df.empty:
#         raise HTTPException(status_code=404, detail="No historical data found")
    
#     # Convert to serializable format
#     data = df.to_dict('records')
    
#     # Convert datetime objects to strings
#     for record in data:
#         if 'timestamp' in record:
#             record['timestamp'] = record['timestamp'].isoformat()
#         if '_id' in record:
#             record['_id'] = str(record['_id'])
    
#     return {
#         "product_id": product_id,
#         "data": data,
#         "count": len(data)
#     }

# @app.get("/market-summary")
# async def get_market_summary():
#     """Get market summary for top 10 products"""
#     top_products = await data_manager.get_top_products_by_volume(10)
    
#     if not top_products:
#         raise HTTPException(status_code=404, detail="No products found")
    
#     summary = []
#     for product_id in top_products:
#         stats = await data_manager.get_product_stats(product_id)
#         if stats:
#             summary.append({
#                 'product_id': product_id,
#                 'price': stats.get('last'),
#                 'volume_24h': stats.get('volume'),
#                 'price_change_24h': stats.get('change'),
#                 'high_24h': stats.get('high'),
#                 'low_24h': stats.get('low')
#             })
    
#     return {
#         "market_summary": summary,
#         "timestamp": datetime.utcnow().isoformat()
#     }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)