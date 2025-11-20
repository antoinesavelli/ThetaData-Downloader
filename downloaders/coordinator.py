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
        
        # PHASE 2: Batch parse all data (OFFLOAD TO THREADS)
        self.logger.info(f"[{date}] Phase 2: Batch parsing {len(raw_data_map)} symbols...")
        parse_start = time.time()
    
        # Concurrency for parsing (tune via config.PARSE_CONCURRENCY)
        import os
        concurrency = getattr(self.config, 'PARSE_CONCURRENCY', max(1, (os.cpu_count() or 4)))
        concurrency = min(concurrency, len(raw_data_map))
        sem = asyncio.Semaphore(concurrency)
        
        async def _parse_symbol(symbol: str, raw_data: Any):
            async with sem:
                try:
                    # Offload blocking parse to a thread so event loop keeps running
                    parsed_df = await asyncio.to_thread(self.parser.parse_market_data, raw_data, symbol)
                    return (symbol, parsed_df, None)
                except Exception as ex:
                    return (symbol, None, ex)
        
        tasks = [asyncio.create_task(_parse_symbol(s, rd)) for s, rd in raw_data_map.items()]
        
        all_dataframes = []
        parse_errors = 0
        try:
            for fut in asyncio.as_completed(tasks):
                try:
                    symbol, parsed_df, exc = await fut
                except asyncio.CancelledError:
                    self.logger.warning(f"[{date}] Parsing cancelled")
                    # cancel outstanding parse tasks
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise
                
                if exc:
                    parse_errors += 1
                    if parse_errors <= 5:
                        self.logger.error(f"[{date}] [{symbol}] Parse error: {exc}")
                    continue
                
                if parsed_df is not None and len(parsed_df) > 0:
                    all_dataframes.append(parsed_df)
        finally:
            # Ensure tasks cleaned up
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        
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
        
        # PHASE 3: Combine and write (offload write to thread)
        self.logger.info(f"[{date}] Phase 3: Combining and writing...")
        write_start = time.time()
        
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        combined_df = combined_df.sort_values(['symbol', 'timestamp'])
        
        # offload parquet write to a thread to avoid blocking the event loop
        file_path_str = await asyncio.to_thread(self.writer.write_date_parquet, date, combined_df, output_dir, overwrite=False)
        
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
        semaphore = asyncio.Semaphore(len(symbols))  # Allow all in this chunk concurrently
    
        async def download_raw_safe(symbol: str) -> tuple[str, Optional[Any]]:
            """Download with individual timeout and error handling."""
            async with semaphore:
                try:
                    # Check null tracker
                    if hasattr(self.parser, 'null_tracker') and self.parser.null_tracker:
                        should_skip, _ = self.parser.null_tracker.should_skip_symbol(symbol, date)
                        if should_skip:
                            return (symbol, None)

                    # ✅ FIXED: Remove coordinator-level timeout to allow MCP client's retry logic to work
                    # The MCP client has its own timeout (60s) and retry logic (3 attempts with exponential backoff)
                    # Adding a timeout here causes premature cancellation and prevents retries
                    raw_data = await self.api.fetch_market_data(
                        symbol,
                        date,
                        start_time=self.config.START_TIME,
                        end_time=self.config.END_TIME
                    )
                
                    # Quick empty check
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
            # ✅ Use gather without return_exceptions for CancelledError
            # This allows CancelledError to propagate naturally
            results = await asyncio.gather(*tasks)
        
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
    
        except Exception as e:
            # Other unexpected exception during gather
            self.logger.error(f"[{date}] Chunk {chunk_idx} error: {e}")
        
            # Cancel remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
        
            # Wait for cleanup
            await asyncio.gather(*tasks, return_exceptions=True)
        
            # Return partial results
            chunk_data = {}
            for task in tasks:
                if task.done() and not task.cancelled():
                    try:
                        symbol, data = task.result()
                        if data is not None:
                            chunk_data[symbol] = data
                    except Exception:
                        pass
        
            return chunk_data
    
        # ✅ Process successful results
        chunk_data = {}
        success_count = 0
    
        for symbol, data in results:
            if data is not None:
                chunk_data[symbol] = data
                success_count += 1
    
        self.logger.info(
            f"[{date}] Chunk {chunk_idx}/{total_chunks}: "
            f"{success_count}/{len(symbols)} symbols downloaded"
        )
    
        return chunk_data

    def _build_error_summary(self, symbols: List[str], error_message: str) -> Dict:
        """Build error summary."""
        return {
            "total": len(symbols),
            "successful": 0,
            "failed": len(symbols),
            "no_data": 0,
            "total_records": 0,
            "dates_processed": 0,
            "dates_successful": 0,
            "dates_failed": 0,
            "error": error_message
        }
