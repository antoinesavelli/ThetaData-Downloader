"""
Download coordinator - OPTIMIZED with better connection handling.
Downloads raw data first, parses in batch later.
"""
import pandas as pd
import json
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import time
import uuid
import traceback

from config.settings import Config
from connectors.thetadata_api import ThetaDataAPI
from processors.data_parser import DataParser
from storage.parquet_writer import ParquetWriter
from utils.helpers import get_trading_dates, calculate_file_size_mb


class DownloadCoordinator:
    """Coordinate download - Download raw first, parse in batch."""
    
    def __init__(self, config: Config, api: ThetaDataAPI, parser: DataParser, 
                 writer: ParquetWriter, logger: logging.Logger):
        """Initialize coordinator with dependencies."""
        self.config = config
        self.api = api
        self.parser = parser
        self.writer = writer
        self.logger = logger
        
        from utils.trading_calendar import TradingCalendar
        self.trading_calendar = TradingCalendar(logger=logger)

    async def download_date_range(self, start_date: str, end_date: str,
                                symbols: List[str], output_dir: str) -> Dict:
        """
        OPTIMIZED: Download all raw data first, then batch parse.
        """
        try:
            download_start_time = time.time()
        
            sorted_symbols = sorted(symbols)
            dates, skipped_dates = self.trading_calendar.get_trading_days_with_skipped(start_date, end_date)
        
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"OPTIMIZED CONSOLIDATED DOWNLOAD")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Strategy: Download RAW → Batch parse → Write consolidated")
            self.logger.info(f"Date range: {start_date} to {end_date}")
            self.logger.info(f"Trading days: {len(dates)}")
            self.logger.info(f"Symbols: {len(sorted_symbols)}")
            self.logger.info(f"Output: ONE parquet per day")
            self.logger.info(f"Concurrency: {self.config.MAX_CONCURRENT_REQUESTS} requests")
        
            if skipped_dates:
                self.logger.info(f"\nSkipped dates:")
                for date, reason in skipped_dates:
                    self.logger.info(f"  ⊗ {date}: {reason}")
        
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"STARTING DOWNLOADS")
            self.logger.info(f"{'='*60}")
        
            results = {
                "dates_processed": 0,
                "dates_successful": 0,
                "dates_failed": 0,
                "total_symbols_downloaded": 0,
                "total_records": 0,
                "total_file_size_mb": 0,
                "total": len(sorted_symbols) * len(dates),
                "successful": 0,
                "failed": 0,
                "no_data": 0,
                "skipped": 0
            }
        
            for date_idx, date in enumerate(dates, 1):
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"Date {date_idx}/{len(dates)}: {date}")
                self.logger.info(f"{'='*60}")
            
                date_result = await self._download_and_parse_day(date, sorted_symbols, output_dir)
            
                results["dates_processed"] += 1
                if date_result["success"]:
                    results["dates_successful"] += 1
                    results["total_symbols_downloaded"] += date_result["symbols_count"]
                    results["total_records"] += date_result["records"]
                    results["total_file_size_mb"] += date_result["file_size_mb"]
                    results["successful"] += date_result["symbols_count"]
                    
                    if date_result.get("skipped"):
                        results["skipped"] += date_result["symbols_count"]
                else:
                    results["dates_failed"] += 1
                    results["failed"] += len(sorted_symbols)
            
                progress_pct = (date_idx / len(dates)) * 100
                self.logger.info(f"Progress: {date_idx}/{len(dates)} ({progress_pct:.1f}%)")

            total_duration = time.time() - download_start_time
        
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"COMPLETED")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Duration: {total_duration:.2f}s ({total_duration/60:.2f} min)")
            self.logger.info(f"Days successful: {results['dates_successful']}")
            self.logger.info(f"Total records: {results['total_records']:,}")
            self.logger.info(f"Total size: {results['total_file_size_mb']:.2f} MB")
            self.logger.info(f"{'='*60}\n")
        
            return results
        
        except asyncio.CancelledError:
            self.logger.warning("Download cancelled by user or system")
            raise
        except Exception as e:
            self.logger.error(f"Download error: {e}")
            return self._build_error_summary(symbols, str(e))

    async def _download_and_parse_day(self, date: str, symbols: List[str], 
                                      output_dir: str) -> Dict:
        """
        OPTIMIZED: Download all raw data, then batch parse and write.
        """
        year, month = date[:4], date[4:6]
        date_path = Path(output_dir) / year / month
        file_path = date_path / f"{date}.parquet"
        
        # Ensure we always attempt a daily reset once this function completes
        try:
            # Check if file exists
            if file_path.exists():
                self.logger.info(f"[{date}] File exists, skipping")
                try:
                    df = pd.read_parquet(file_path)
                    file_size = file_path.stat().st_size / (1024 * 1024)
                    return {
                        "success": True,
                        "date": date,
                        "symbols_count": df['symbol'].nunique(),
                        "records": len(df),
                        "file_size_mb": file_size,
                        "skipped": True
                    }
                except Exception as e:
                    self.logger.warning(f"[{date}] Error reading existing file: {e}")
        
            # PHASE 1: Download all raw data (NO PARSING)
            self.logger.info(f"[{date}] Phase 1: Downloading raw data for {len(symbols)} symbols...")
            download_start = time.time()
            
            try:
                raw_data_map = await self._download_all_raw_chunked(symbols, date)
            except asyncio.CancelledError:
                self.logger.warning(f"[{date}] Download cancelled")
                raise
            
            download_duration = time.time() - download_start
            self.logger.info(f"[{date}] Downloaded {len(raw_data_map)} symbols in {download_duration:.2f}s")
            
            if not raw_data_map:
                self.logger.warning(f"[{date}] No data downloaded")
                return {
                    "success": False,
                    "date": date,
                    "symbols_count": 0,
                    "records": 0,
                    "file_size_mb": 0,
                    "error": "No data"
                }
            
            # PHASE 2: Batch parse all data (synchronous parsing)
            self.logger.info(f"[{date}] Phase 2: Batch parsing {len(raw_data_map)} symbols...")
            parse_start = time.time()
        
            all_dataframes, parse_errors = await self._parse_all_concurrent(raw_data_map, date)
            
            parse_duration = time.time() - parse_start
            self.logger.info(f"[{date}] Parsed {len(all_dataframes)}/{len(raw_data_map)} symbols in {parse_duration:.2f}s")
            
            if parse_errors > 5:
                self.logger.warning(f"[{date}] {parse_errors} total parse errors (showing first 5)")
            
            if not all_dataframes:
                self.logger.warning(f"[{date}] No valid data after parsing")
                return {
                    "success": False,
                    "date": date,
                    "symbols_count": 0,
                    "records": 0,
                    "file_size_mb": 0,
                    "error": "No valid data"
                }
            
            # PHASE 3: Combine and write (synchronous write)
            self.logger.info(f"[{date}] Phase 3: Combining and writing...")
            write_start = time.time()
            
            # Optimize concatenation (replace line 144)
            # Pre-sort each DataFrame before concat for faster merge-sort
            for df in all_dataframes:
                df.sort_values('timestamp', inplace=True)

            # Concat is faster with pre-sorted data
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            # Final sort only by symbol since timestamp is already sorted within each symbol
            combined_df.sort_values('symbol', inplace=True, kind='mergesort')  # Stable sort
            
            # Write synchronously - no threading
            file_path_str = await self._write_parquet_async(date, combined_df, output_dir)
            
            write_duration = time.time() - write_start
            
            if file_path_str:
                file_size = Path(file_path_str).stat().st_size / (1024 * 1024)
                total_duration = download_duration + parse_duration + write_duration
                
                self.logger.info(
                    f"[{date}] ✓ Complete in {total_duration:.2f}s "
                    f"(DL: {download_duration:.1f}s, Parse: {parse_duration:.1f}s, Write: {write_duration:.1f}s)"
                )
                
                return {
                    "success": True,
                    "date": date,
                    "symbols_count": combined_df['symbol'].nunique(),
                    "records": len(combined_df),
                    "file_size_mb": file_size,
                    "skipped": False
                }
            else:
                return {
                    "success": False,
                    "date": date,
                    "symbols_count": 0,
                    "records": 0,
                    "file_size_mb": 0,
                    "error": "Write failed"
                }
        finally:
            # Perform a daily full reset of MCP client to avoid long-lived session/resource issues.
            # This runs regardless of success/failure and will not raise.
            try:
                if hasattr(self.api, "reset") and callable(getattr(self.api, "reset")):
                    # Do not force reconnect by default; warm reconnect can be enabled by passing True
                    await self.api.reset(force_reconnect=False)
            except Exception as ex:
                self.logger.debug(f"[{date}] MCP client daily reset failed: {ex}")

    async def _download_all_raw_chunked(self, symbols: List[str], date: str) -> Dict[str, Any]:
        """
        Download raw API responses in CHUNKS to avoid connection overload.
        Processes symbols in batches to prevent timeout cascades.
        """
        chunk_size = self.config.MAX_CONCURRENT_REQUESTS
        all_raw_data = {}
        
        # Split symbols into chunks
        symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
        
        self.logger.info(f"[{date}] Downloading in {len(symbol_chunks)} chunks of {chunk_size}")
        
        for chunk_idx, symbol_chunk in enumerate(symbol_chunks, 1):
            try:
                # Download this chunk with proper cancellation handling
                chunk_data = await self._download_chunk(symbol_chunk, date, chunk_idx, len(symbol_chunks))
                all_raw_data.update(chunk_data)
                
                # Small delay between chunks to avoid overwhelming the server
                if chunk_idx < len(symbol_chunks):
                    await asyncio.sleep(0.5)
                    
            except asyncio.CancelledError:
                self.logger.warning(f"[{date}] Chunk {chunk_idx} cancelled")
                raise  # Propagate cancellation up
            except Exception as e:
                self.logger.error(f"[{date}] Error in chunk {chunk_idx}: {e}")
                # Continue with next chunk even if this one failed
                continue
    
        return all_raw_data

    async def _download_chunk(self, symbols: List[str], date: str, 
                             chunk_idx: int, total_chunks: int) -> Dict[str, Any]:
        """
        Download a single chunk of symbols with proper error isolation.
        """
        # Use configured global concurrency rather than allowing the whole chunk at once.
        semaphore = asyncio.Semaphore(getattr(self.config, 'MAX_CONCURRENT_REQUESTS', len(symbols)))
        rate_limit_delay = getattr(self.config, 'RATE_LIMIT_DELAY', 0.0)
    
        async def download_raw_safe(symbol: str) -> tuple[str, Optional[Any]]:
            """Download with individual timeout and error handling."""
            try:
                # Check null tracker BEFORE acquiring the semaphore to avoid holding slots for skipped symbols
                if hasattr(self.parser, 'null_tracker') and self.parser.null_tracker:
                    should_skip, _ = self.parser.null_tracker.should_skip_symbol(symbol, date)
                    if should_skip:
                        return (symbol, None)
        
                req_id = f"{symbol}-{date}-{uuid.uuid4().hex[:8]}"
                self.logger.debug(f"[{req_id}] download_raw_safe START symbol={symbol} date={date} task={asyncio.current_task().get_name() if asyncio.current_task() else None}")
        
                # Call the API while holding the semaphore, but release it immediately after the call.
                call_start = time.monotonic()
                try:
                    async with semaphore:
                        raw_data = await self.api.fetch_market_data(
                            symbol,
                            date,
                            start_time=self.config.START_TIME,
                            end_time=self.config.END_TIME
                        )
                    call_elapsed = time.monotonic() - call_start
                    self.logger.debug(f"[{req_id}] fetch_market_data returned (elapsed={call_elapsed:.3f}s) data_present={bool(raw_data)}")
                except asyncio.CancelledError:
                    call_elapsed = time.monotonic() - call_start
                    self.logger.warning(f"[{req_id}] download_raw_safe CancelledError after {call_elapsed:.3f}s; cancelling outstanding tasks")
                    # dump a small snapshot (stack) for debugging
                    for t in asyncio.all_tasks():
                        try:
                            n = t.get_name() if hasattr(t, "get_name") else getattr(t, "name", None)
                        except Exception:
                            n = None
                    raise
                except Exception as e:
                    # Log and treat as no-data for this symbol
                    self.logger.debug(f"[{req_id}] fetch_market_data exception: {type(e).__name__} {str(e)[:300]}")
                    return (symbol, None)
        
                # Rate limiting: small delay AFTER releasing the semaphore so we don't hold the slot while sleeping.
                if rate_limit_delay > 0:
                    try:
                        await asyncio.sleep(rate_limit_delay)
                    except asyncio.CancelledError:
                        # If cancelled during sleep, propagate immediately — semaphore is already released.
                        raise
        
                # Quick empty checks
                if not raw_data:
                    return (symbol, None)
        
                if isinstance(raw_data, list) and len(raw_data) == 0:
                    return (symbol, None)
        
                if isinstance(raw_data, str):
                    lines = raw_data.strip().split('\n')
                    if len(lines) <= 1:
                        return (symbol, None)
        
                return (symbol, raw_data)
        
            except asyncio.CancelledError:
                # ✅ Re-raise immediately - DO NOT CATCH
                self.logger.debug(f"[{symbol}] [{date}] Cancelled")
                raise
            except Exception as e:
                # Only log unexpected errors
                error_type = type(e).__name__
                if 'Closed' not in error_type and 'Cancel' not in error_type:
                    self.logger.debug(f"[{symbol}] [{date}] {error_type}")
                return (symbol, None)
    
        # Download all in this chunk
        tasks = [asyncio.create_task(download_raw_safe(sym)) for sym in symbols]
    
        try:
            self.logger.debug(f"[{date}] Chunk {chunk_idx} starting gather for {len(tasks)} tasks; task_names={[t.get_name() if hasattr(t,'get_name') else getattr(t,'name',None) for t in tasks]}")
            
            # ✅ FIXED: Use return_exceptions=True to prevent cascade cancellations
            # This isolates individual task failures and preserves successful downloads
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        except asyncio.CancelledError:
            # ✅ Cancel all outstanding tasks
            self.logger.debug(f"[{date}] Chunk {chunk_idx} cancelled - cancelling {len(tasks)} tasks")
        
            for task in tasks:
                if not task.done():
                    task.cancel()
        
            # Wait for all tasks to complete cancellation
            await asyncio.gather(*tasks, return_exceptions=True)
        
            # Re-raise to propagate cancellation up the chain
            raise
        
        self.logger.debug(f"[{date}] Chunk {chunk_idx} gather completed; results={len(results)}")
    
        # ✅ Process results, handling both successful and failed tasks
        chunk_data = {}
        success_count = 0
        error_count = 0
    
        for result in results:
            # Handle exceptions (including CancelledError from individual tasks)
            if isinstance(result, Exception):
                error_count += 1
                if error_count <= 3:
                    error_type = type(result).__name__
                    self.logger.debug(f"[{date}] Chunk {chunk_idx} task failed: {error_type}")
                continue
            
            # Handle successful results
            symbol, data = result
            if data is not None:
                chunk_data[symbol] = data
                success_count += 1
        
        if error_count > 3:
            self.logger.debug(f"[{date}] Chunk {chunk_idx} had {error_count} task errors (showing first 3)")
    
        self.logger.info(
            f"[{date}] Chunk {chunk_idx}/{total_chunks}: "
            f"{success_count}/{len(symbols)} symbols downloaded"
        )
    
        return chunk_data

    # Replace synchronous parsing loop with concurrent parsing
    async def _parse_all_concurrent(self, raw_data_map: Dict[str, Any], date: str) -> List[pd.DataFrame]:
        """Parse multiple symbols concurrently using thread pool."""
        import concurrent.futures
        from functools import partial
        
        loop = asyncio.get_running_loop()
        all_dataframes = []
        parse_errors = 0
        
        # Create partial function for thread pool
        parse_func = partial(self.parser.parse_market_data)
        
        # Process in batches to avoid memory overload
        batch_size = 50
        symbols = list(raw_data_map.keys())
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                batch_tasks = []
                
                for symbol in batch_symbols:
                    raw_data = raw_data_map[symbol]
                    # Submit parsing task to thread pool
                    task = loop.run_in_executor(executor, parse_func, raw_data, symbol)
                    batch_tasks.append((symbol, task))
                
                # Wait for batch to complete
                for symbol, task in batch_tasks:
                    try:
                        parsed_df = await task
                        if parsed_df is not None and len(parsed_df) > 0:
                            all_dataframes.append(parsed_df)
                    except Exception as ex:
                        parse_errors += 1
                        if parse_errors <= 5:
                            self.logger.error(f"[{date}] [{symbol}] Parse error: {ex}")
        
        return all_dataframes, parse_errors

    def _build_error_summary(self, symbols: List[str], error_message: str) -> Dict:
        """Build error summary with all required keys."""
        return {
            "total": len(symbols),
            "successful": 0,
            "failed": len(symbols),
            "no_data": 0,
            "skipped": 0,
            "total_records": 0,
            "total_file_size_mb": 0,
            "dates_processed": 0,
            "dates_successful": 0,
            "dates_failed": 0,
            "total_symbols_downloaded": 0,
            "error": error_message
        }

    # Replace synchronous write with async write
    async def _write_parquet_async(self, date: str, combined_df: pd.DataFrame, output_dir: str) -> Optional[str]:
        """Write parquet file asynchronously using thread pool."""
        loop = asyncio.get_running_loop()
        
        # Run write operation in thread pool to avoid blocking
        file_path = await loop.run_in_executor(
            None,
            self.writer.write_date_parquet,
            date,
            combined_df,
            output_dir,
            False  # overwrite
        )
        
        return file_path
