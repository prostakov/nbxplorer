using Dapper;
using Microsoft.Extensions.Logging;
using NBitcoin.Protocol;
using NBitcoin.RPC;
using NBitcoin;
using NBXplorer.Backend;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace NBXplorer
{
    /// <summary>
    /// Specialized indexer functionality for Haroldcoin
    /// </summary>
    public static class HaroldcoinIndexer
    {
        /// <summary>
        /// Specialized sync loop for Haroldcoin to handle its P2P protocol differences
        /// </summary>
        public static async Task HaroldcoinSyncLoop(Indexer indexer, DbConnectionHelper dbConn, RPCClient rpcClient, ILogger logger, CancellationToken token)
        {
            logger.LogInformation($"Starting specialized Haroldcoin synchronization loop");

            try
            {
                // Get the current blockchain info to start with
                var blockchainInfo = await rpcClient.GetBlockchainInfoAsyncEx();
                
                // First, check the actual data in our database to ensure we start from the right height
                var highestBlockInDb = await GetHighestBlockInDatabase(dbConn, "HRLD", logger);
                
                // If database is empty, start from zero regardless of what indexer.SyncHeight says
                var startHeight = highestBlockInDb.HasValue ? highestBlockInDb.Value : 0;
                var targetHeight = blockchainInfo.Headers;
                
                logger.LogInformation($"Haroldcoin: Syncing from height {startHeight} to {targetHeight}");
                
                // Always log the sync message if database is empty
                if (startHeight < targetHeight || !highestBlockInDb.HasValue)
                {
                    logger.LogInformation("Haroldcoin: Using direct RPC sync for more reliable synchronization");
                }
                
                // Call the sync method, passing the database check result to avoid duplicate query
                await DirectSyncBlocks(indexer, dbConn, rpcClient, logger, token, startHeight, targetHeight, highestBlockInDb);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in Haroldcoin sync loop");
            }
        }
        
        /// <summary>
        /// Process blocks directly using RPC without going through P2P protocol
        /// </summary>
        public static async Task DirectSyncBlocks(Indexer indexer, DbConnectionHelper dbConn, RPCClient rpcClient, ILogger logger, CancellationToken token, 
            long? overrideStartHeight = null, long? overrideEndHeight = null, long? highestBlockInDb = null)
        {
            try
            {
                // Get current blockchain info
                var blockchainInfo = await rpcClient.GetBlockchainInfoAsyncEx();
                
                // Use provided values or get from blockchain/indexer
                var startHeight = overrideStartHeight ?? indexer.SyncHeight ?? 0;
                var endHeight = overrideEndHeight ?? blockchainInfo.Headers;
                
                // Special case: If the database is empty but we think we're synced, force sync from genesis
                if (startHeight == endHeight && highestBlockInDb.HasValue == false)
                {
                    logger.LogWarning("Database appears to be empty but system thinks it's synced. Forcing sync from genesis.");
                    startHeight = 0;
                    endHeight = Math.Min(1000, blockchainInfo.Headers);  // First 1000 blocks to start
                }
                
                logger.LogInformation($"Haroldcoin: Direct sync from height {startHeight} to {endHeight}");
                
                // If we're already at the end height, wait for new blocks
                if (startHeight >= endHeight)
                {
                    // Special case: If database is actually empty, don't return early
                    if (highestBlockInDb.HasValue == false)
                    {
                        logger.LogWarning("Database is empty, will continue with sync despite height calculations");
                    }
                    else
                    {
                        logger.LogInformation($"Haroldcoin: Already at the latest block height. Will check again in 30 seconds.");
                        
                        // Update state and wait for new blocks
                        await indexer.UpdateStateWithoutNode();
                        
                        // Check for blocks with height 0 as a maintenance task while waiting
                        await FixBlockHeightsInDatabase(dbConn, rpcClient, "HRLD", logger);
                        
                        // Successfully go to Ready state and return
                        logger.LogInformation($"Haroldcoin: Direct sync completed to height {endHeight}. Waiting for new blocks.");
                        return;
                    }
                }
                
                // Check for blocks with height 0 (potential issue)
                await FixBlockHeightsInDatabase(dbConn, rpcClient, "HRLD", logger);
                
                // Initialize the node tip to prevent NullReferenceException in SaveMatches
                await EnsureNodeTipInitialized(indexer, rpcClient, endHeight, logger);
                
                // Calculate the number of blocks to process for progress reporting
                var totalBlocksToProcess = endHeight - startHeight;
                var processedCount = 0;
                
                // Process one block at a time for maximum reliability
                for (int height = (int)startHeight + 1; height <= endHeight; height++)
                {
                    // Retry logic for important operations
                    int maxRetries = 3;
                    int retryDelay = 500; // milliseconds
                    bool blockProcessed = false;
                    
                    for (int retryCount = 0; retryCount < maxRetries && !blockProcessed; retryCount++)
                    {
                        try
                        {
                            // If this is a retry, log it and wait before trying again
                            if (retryCount > 0)
                            {
                                logger.LogWarning($"Haroldcoin: Retry #{retryCount} for block at height {height}");
                                await Task.Delay(retryDelay * retryCount, token);
                            }
                            
                            // Get the block hash at this height - critical step
                            var hash = await rpcClient.GetBlockHashAsync(height);
                            if (hash == null)
                            {
                                logger.LogWarning($"Haroldcoin: Could not get hash for block at height {height}");
                                continue; // Try again if we have retries left
                            }
                            
                            // Log progress every 100 blocks or for key milestones
                            processedCount++;
                            if (height % 100 == 0 || height == (int)startHeight + 1 || height == endHeight)
                            {
                                double progressPct = (processedCount * 100.0) / totalBlocksToProcess;
                                logger.LogInformation($"Haroldcoin: Syncing block {height}/{endHeight} - {progressPct:F2}% complete");
                            }
                            
                            // Get the block - critical step
                            var block = await rpcClient.GetBlockAsync(hash);
                            if (block == null)
                            {
                                logger.LogWarning($"Haroldcoin: Could not get block at height {height} (hash: {hash})");
                                continue; // Try again if we have retries left
                            }
                            
                            // Create SlimChainedBlock directly without using GetBlockHeaderAsyncEx
                            uint256 previousBlockHash = null;
                            if (height > 0)
                            {
                                // For non-genesis blocks, get the previous hash from the block itself
                                previousBlockHash = block.Header.HashPrevBlock;
                            }
                            
                            var slimChainedBlock = new SlimChainedBlock(
                                hash: hash,
                                prev: height == 0 ? uint256.Zero : previousBlockHash,
                                height: height);
                            
                            logger.LogInformation($"Haroldcoin: Created header for block at height {height} (hash: {hash}, prev: {previousBlockHash})");
                            
                            // Save the block
                            await indexer.SaveMatches(dbConn, block, slimChainedBlock);
                            
                            // Mark as successfully processed
                            blockProcessed = true;
                            
                            // Save progress periodically
                            if (height % 10 == 0 || height == endHeight)
                            {
                                await indexer.SaveProgress(dbConn);
                                await indexer.UpdateStateWithoutNode();
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, $"Error processing block at height {height}, retry {retryCount+1}/{maxRetries}");
                            
                            // If this was our last retry, and it's still failing
                            if (retryCount == maxRetries - 1)
                            {
                                logger.LogWarning($"Haroldcoin: Failed to process block at height {height} after {maxRetries} attempts, skipping to next height");
                                
                                // Add a slightly longer delay before moving to the next block
                                await Task.Delay(retryDelay * 2, token);
                            }
                        }
                    }
                }
                
                // Final save of progress
                await indexer.SaveProgress(dbConn);
                await indexer.UpdateStateWithoutNode();
                
                // Fix any remaining blocks with incorrect heights
                await FixBlockHeightsInDatabase(dbConn, rpcClient, "HRLD", logger);
                
                logger.LogInformation($"Haroldcoin: Direct sync completed to height {endHeight}");
                
                // Add an extra consistency check for blocks that might have been missed
                await CheckForMissingBlocks(indexer, dbConn, rpcClient, "HRLD", (int)startHeight, (int)endHeight, logger);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in direct sync");
                throw;
            }
        }

        /// <summary>
        /// Ensure that _NodeTip is initialized to prevent NullReferenceException
        /// </summary>
        private static async Task EnsureNodeTipInitialized(Indexer indexer, RPCClient rpcClient, long endHeight, ILogger logger)
        {
            // This method requires access to a private field in Indexer
            // We should expose this through a public method in the Indexer class
            try
            {
                if (indexer.HasNullNodeTip())
                {
                    // Get the best block hash and create a SlimChainedBlock for it
                    var bestBlockHash = await rpcClient.GetBestBlockHashAsync();
                    indexer.InitializeNodeTip(new SlimChainedBlock(bestBlockHash, uint256.Zero, (int)endHeight));
                    logger.LogInformation($"Initialized _NodeTip with height {endHeight} and hash {bestBlockHash}");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error initializing node tip");
            }
        }

        /// <summary>
        /// Checks for missing blocks in the height range and attempts to retrieve them
        /// </summary>
        public static async Task CheckForMissingBlocks(Indexer indexer, DbConnectionHelper dbConn, RPCClient rpcClient, string cryptoCode, int startHeight, int endHeight, ILogger logger)
        {
            try
            {
                logger.LogInformation($"Checking for missing blocks in range {startHeight}-{endHeight}...");
                
                // Get all heights in the database for our crypto code
                var existingHeights = (await dbConn.Connection.QueryAsync<int>(
                    "SELECT height FROM blks WHERE code=@code AND height BETWEEN @startHeight AND @endHeight ORDER BY height",
                    new { code = cryptoCode, startHeight, endHeight })).ToHashSet();
                
                // Find missing heights
                var missingHeights = new List<int>();
                for (int height = startHeight + 1; height <= endHeight; height++)
                {
                    if (!existingHeights.Contains(height))
                    {
                        missingHeights.Add(height);
                    }
                }
                
                if (missingHeights.Count == 0)
                {
                    logger.LogInformation("No missing blocks found in the database.");
                    return;
                }
                
                logger.LogWarning($"Found {missingHeights.Count} missing blocks in the database. Attempting to retrieve them...");
                
                // Process missing blocks
                int processedCount = 0;
                foreach (var height in missingHeights)
                {
                    try
                    {
                        // Get the block hash
                        var hash = await rpcClient.GetBlockHashAsync(height);
                        if (hash == null)
                        {
                            logger.LogWarning($"Could not get hash for missing block at height {height}");
                            continue;
                        }
                        
                        // Get the block
                        var block = await rpcClient.GetBlockAsync(hash);
                        if (block == null)
                        {
                            logger.LogWarning($"Could not get missing block at height {height} (hash: {hash})");
                            continue;
                        }
                        
                        // Create SlimChainedBlock
                        uint256 previousBlockHash = null;
                        if (height > 0)
                        {
                            previousBlockHash = block.Header.HashPrevBlock;
                        }
                        
                        var slimChainedBlock = new SlimChainedBlock(
                            hash: hash,
                            prev: height == 0 ? uint256.Zero : previousBlockHash,
                            height: height);
                        
                        // Save the block
                        await indexer.SaveMatches(dbConn, block, slimChainedBlock);
                        processedCount++;
                        
                        logger.LogInformation($"Successfully processed missing block at height {height} (hash: {hash})");
                        
                        // Save progress periodically
                        if (processedCount % 10 == 0)
                        {
                            await indexer.SaveProgress(dbConn);
                            await indexer.UpdateStateWithoutNode();
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, $"Error processing missing block at height {height}");
                    }
                }
                
                // Final save after processing missing blocks
                if (processedCount > 0)
                {
                    await indexer.SaveProgress(dbConn);
                    await indexer.UpdateStateWithoutNode();
                    logger.LogInformation($"Successfully processed {processedCount} missing blocks");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error checking for missing blocks");
            }
        }
        
        /// <summary>
        /// Checks for blocks with height 0 in the database and corrects them based on their position in the chain
        /// </summary>
        public static async Task FixBlockHeightsInDatabase(DbConnectionHelper dbConn, RPCClient rpcClient, string cryptoCode, ILogger logger)
        {
            try
            {
                logger.LogInformation("Checking for blocks with incorrect heights in database...");
                
                // Query to find blocks with height 0 (except genesis block)
                var blocksWithZeroHeight = await dbConn.Connection.QueryAsync<(string blk_id, string prev_id)>(
                    "SELECT blk_id, prev_id FROM blks WHERE code=@code AND height=0 AND prev_id IS NOT NULL",
                    new { code = cryptoCode });
                    
                var blocks = blocksWithZeroHeight.ToList();
                if (blocks.Count == 0)
                {
                    logger.LogInformation("No blocks with incorrect heights found.");
                    return;
                }
                
                logger.LogWarning($"Found {blocks.Count} blocks with height 0 that need fixing");
                
                // Create a lookup by previous hash
                var blocksByPrev = blocks.ToDictionary(b => b.prev_id, b => b.blk_id);
                
                // Get genesis block
                var genesisHash = await rpcClient.GetBlockHashAsync(0);
                if (genesisHash == null)
                {
                    logger.LogWarning("Could not get genesis block hash");
                    return;
                }
                
                // Get the current height from the node
                var blockchainInfo = await rpcClient.GetBlockchainInfoAsyncEx();
                
                // Fix blocks by traversing the chain from genesis
                var currentHash = genesisHash.ToString();
                int height = 0;
                int fixedCount = 0;
                
                while (blocksByPrev.TryGetValue(currentHash, out var nextHash))
                {
                    height++;
                    try
                    {
                        // Update the height in the database
                        await dbConn.Connection.ExecuteAsync(
                            "UPDATE blks SET height=@height WHERE code=@code AND blk_id=@blk_id",
                            new { code = cryptoCode, blk_id = nextHash, height });
                            
                        currentHash = nextHash;
                        fixedCount++;
                        
                        if (fixedCount % 100 == 0)
                        {
                            logger.LogInformation($"Fixed {fixedCount} block heights so far");
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, $"Error fixing height for block {nextHash}");
                        break;
                    }
                }
                
                logger.LogInformation($"Fixed heights for {fixedCount} blocks");
                
                // One more check for any blocks missed by the chain traversal
                var remainingZeroHeights = await dbConn.Connection.QueryAsync<int>(
                    "SELECT COUNT(*) FROM blks WHERE code=@code AND height=0 AND prev_id IS NOT NULL",
                    new { code = cryptoCode });
                    
                if (remainingZeroHeights.First() > 0)
                {
                    logger.LogWarning($"There are still {remainingZeroHeights.First()} blocks with height 0 that couldn't be fixed automatically");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error checking for blocks with incorrect heights");
            }
        }

        /// <summary>
        /// Gets the highest block height in the database
        /// </summary>
        private static async Task<long?> GetHighestBlockInDatabase(DbConnectionHelper dbConn, string cryptoCode, ILogger logger)
        {
            try
            {
                var result = await dbConn.Connection.QueryFirstOrDefaultAsync<int?>(
                    "SELECT MAX(height) FROM blks WHERE code=@code AND confirmed IS TRUE",
                    new { code = cryptoCode });
                    
                if (result.HasValue)
                {
                    logger.LogInformation($"Found highest block in database at height {result.Value}");
                    return result.Value;
                }
                
                logger.LogInformation("No blocks found in database");
                return null;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error getting highest block from database");
                return null;
            }
        }
    }
} 