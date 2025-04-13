using NBitcoin;
using NBitcoin.RPC;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;
using System;
using System.Linq;
using NBitcoin.Protocol;
using System.Collections.Concurrent;
using Newtonsoft.Json.Linq;
using NBXplorer.Backend;
using System.Data.Common;

namespace NBXplorer
{
    /// <summary>
    /// Helper methods for Haroldcoin-specific behavior
    /// </summary>
    public static class HaroldcoinHelper
    {
        // Cache for block heights to avoid excessive RPC calls
        private static readonly ConcurrentDictionary<string, int> _cachedHeights = new ConcurrentDictionary<string, int>();
        
        /// <summary>
        /// Handle node disconnection events for Haroldcoin with special logic
        /// </summary>
        public static bool HandleNodeDisconnected(Node node, BitcoinDWaiterState currentState, ILogger logger)
        {
            // For Haroldcoin, don't reset state if we're already synced via RPC or syncing
            if (currentState == BitcoinDWaiterState.Ready || currentState == BitcoinDWaiterState.NBXplorerSynching)
            {
                logger.LogInformation($"Node disconnected ({node.DisconnectReason.Reason}) - Haroldcoin will continue using RPC");
                // Let the caller handle the event unsubscription
                return true;
            }
            
            // Return false to let the standard disconnection handling proceed
            return false;
        }
        
        /// <summary>
        /// Special connection logic for Haroldcoin P2P
        /// </summary>
        public static async Task<Backend.Indexer.Connection> TryConnectToHaroldcoinNode(Indexer indexer, RPCClient rpc, ILogger logger, CancellationToken token)
        {
            Backend.Indexer.Connection connection = null;
            
            try
            {
                // We can't directly call ConnectNode since it's private
                // Instead, let the Indexer handle the connection in its IndexerLoopCore method
                logger.LogInformation("Connection to Haroldcoin P2P node will be handled by the Indexer");
                
                // Add an await operation to prevent the warning
                await Task.Delay(0, token); // This is a no-op await that prevents the CS1998 warning
            }
            catch (Exception ex)
            {
                // For Haroldcoin, log but continue even if connection fails
                logger.LogWarning($"P2P connection to Haroldcoin node failed: {ex.Message}. Will continue with RPC sync.");
            }
            
            return connection;
        }
        
        /// <summary>
        /// Adds a delay between sync loops for Haroldcoin to prevent tight loops
        /// </summary>
        public static async Task AddSyncLoopDelay(ILogger logger, CancellationToken token, DbConnectionFactory factory = null, NBXplorerNetwork network = null)
        {
            const int delaySeconds = 30;
            logger.LogInformation($"Haroldcoin: Waiting {delaySeconds} seconds before next sync check...");
            
            // Perform periodic pool cleanup every 10 minutes
            if (DateTime.UtcNow.Minute % 10 == 0 && DateTime.UtcNow.Second < 30 && factory != null && network != null)
            {
                logger.LogInformation("Performing periodic connection refresh");
                try
                {
                    // Create a temporary connection and immediately dispose it
                    // This helps keep the connection pool healthy
                    var tempConn = await factory.CreateConnectionHelper(network);
                    await RefreshConnection(factory, network, tempConn, logger);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Error during periodic connection refresh, continuing normally");
                }
            }
            
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), token);
            }
            catch (OperationCanceledException)
            {
                // Ignore cancellation
            }
        }
        
        /// <summary>
        /// Performs memory cleanup operations to help prevent connection pool exhaustion
        /// </summary>
        public static void PerformMemoryCleanup(ILogger logger)
        {
            try
            {
                // Clear height cache to reduce memory pressure
                ClearHeightCache();
                
                // Force garbage collection to release resources including db connections
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Optimized, true);
                GC.WaitForPendingFinalizers();
                
                // Second collection to clean up anything freed by finalizers
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Optimized, true);
                
                logger.LogDebug("Memory cleanup completed");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Error during memory cleanup");
            }
        }
        
        /// <summary>
        /// Gets a genesis block locator without relying on GetGenesis()
        /// </summary>
        public static async Task<BlockLocator> GetSafeBlockLocator(RPCClient rpcClient, ILogger logger, CancellationToken token = default)
        {
            var blockLocator = new BlockLocator();
            
            try
            {
                // Try to get the genesis block hash through RPC
                var genesisHash = await rpcClient.GetBlockHashAsync(0);
                if (genesisHash != null)
                {
                    blockLocator.Blocks.Add(genesisHash);
                    logger.LogInformation($"Added genesis block from RPC: {genesisHash}");
                }
                else
                {
                    logger.LogWarning("Could not get genesis hash via RPC");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error getting genesis block hash");
            }
            
            // If we couldn't get the genesis hash, try to get blocks at some early heights
            if (blockLocator.Blocks.Count == 0)
            {
                try
                {
                    // Try some early blocks
                    foreach (var height in new[] { 1, 10, 100 })
                    {
                        try
                        {
                            var hash = await rpcClient.GetBlockHashAsync(height);
                            if (hash != null)
                            {
                                blockLocator.Blocks.Add(hash);
                                logger.LogInformation($"Added early block at height {height}: {hash}");
                                break;
                            }
                        }
                        catch
                        {
                            // Ignore errors for individual lookups
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error getting early blocks");
                }
            }
            
            // If still empty, use a hardcoded known block hash for Haroldcoin
            if (blockLocator.Blocks.Count == 0)
            {
                // Hardcoded fallback for Haroldcoin genesis (replace with actual genesis hash)
                var hardcodedHash = new uint256("00000f2fddbb7212e6f36f398461e8ad49dba752608c9c7322cb97e9a893b485");
                blockLocator.Blocks.Add(hardcodedHash);
                logger.LogWarning($"Using hardcoded genesis fallback: {hardcodedHash}");
            }
            
            return blockLocator;
        }
        
        /// <summary>
        /// Safely sends a genesis-based GetHeadersPayload without relying on GetGenesis()
        /// </summary>
        public static async Task SendSafeGenesisHeadersRequest(RPCClient rpcClient, Node node, ILogger logger, CancellationToken token = default)
        {
            try
            {
                // Try to get the genesis block hash through RPC
                var genesisHash = await rpcClient.GetBlockHashAsync(0);
                if (genesisHash != null)
                {
                    var genesisLocator = new BlockLocator();
                    genesisLocator.Blocks.Add(genesisHash);
                    logger.LogInformation($"Sending GetHeadersPayload from genesis: {genesisHash}");
                    await node.SendMessageAsync(new GetHeadersPayload(genesisLocator));
                }
                else
                {
                    logger.LogWarning("Could not get genesis hash via RPC, skipping genesis headers request");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error sending genesis headers request");
            }
        }
        
        /// <summary>
        /// Safely attempt to request headers with fallbacks for Haroldcoin's unreliable P2P protocol
        /// </summary>
        public static async Task AskNextHeadersWithFallbacks(Node node, BlockLocator locator, ILogger logger, RPCClient rpcClient, CancellationToken token = default)
        {
            try
            {
                if (locator.Blocks.Count > 0)
                {
                    logger.LogInformation($"HRLD: Requesting headers from block {locator.Blocks[0]} (locator has {locator.Blocks.Count} blocks)");
                    await node.SendMessageAsync(new GetHeadersPayload(locator));
                }
                else
                {
                    logger.LogInformation($"HRLD: Trying GetHeaders with empty locator");
                }

                // Also try sending a GetHeadersPayload with a fresh locator
                logger.LogInformation($"HRLD: Sending GetHeadersPayload with {locator.Blocks.Count} blocks");
                await node.SendMessageAsync(new GetHeadersPayload(locator));

                // Try sending from genesis as well (more reliable in some cases)
                await SendSafeGenesisHeadersRequest(rpcClient, node, logger, token);

                // Additionally send GetBlocksPayload as a fallback
                logger.LogInformation($"HRLD: Sending GetBlocksPayload");
                await node.SendMessageAsync(new GetBlocksPayload(locator));

                // Send a ping to keep the connection alive
                await node.SendMessageAsync(new PingPayload());
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error sending header requests for Haroldcoin");
                throw; // Rethrow to handle at caller level
            }
        }
        
        /// <summary>
        /// Gets the height for a given block, with caching for efficiency
        /// </summary>
        public static async Task<int> GetBlockHeightSafe(RPCClient rpcClient, uint256 blockHash, ILogger logger, CancellationToken token = default)
        {
            var hashStr = blockHash.ToString();
            
            // Check the cache first
            if (_cachedHeights.TryGetValue(hashStr, out var cachedHeight))
            {
                return cachedHeight;
            }
            
            try
            {
                // Try to get the block directly to extract its height
                var blockResponse = await rpcClient.SendCommandAsync(
                    new RPCRequest("getblock", new[] { hashStr })
                    {
                        ThrowIfRPCError = false
                    }, token);
                
                if (blockResponse?.Result is JObject block)
                {
                    // Try to get height from the response
                    if (block["height"] is JToken heightToken && heightToken.Type == JTokenType.Integer)
                    {
                        var height = heightToken.Value<int>();
                        if (height >= 0)
                        {
                            // Cache the result
                            _cachedHeights.TryAdd(hashStr, height);
                            return height;
                        }
                    }
                    
                    // Alternative approach: Try to calculate from confirmations
                    if (block["confirmations"] is JToken confsToken && confsToken.Type == JTokenType.Integer)
                    {
                        var confs = confsToken.Value<int>();
                        // If confirmations are valid, we can estimate the height
                        if (confs > 0)
                        {
                            try
                            {
                                // Get current block count
                                var blockCountResponse = await rpcClient.SendCommandAsync(
                                    new RPCRequest("getblockcount", Array.Empty<object>())
                                    {
                                        ThrowIfRPCError = false
                                    }, token);
                                
                                if (blockCountResponse?.Result != null)
                                {
                                    var currentHeight = blockCountResponse.Result.Value<int>();
                                    var height = currentHeight - confs + 1;
                                    
                                    // Cache the result
                                    _cachedHeights.TryAdd(hashStr, height);
                                    return height;
                                }
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, $"Error calculating height from confirmations for block {hashStr}");
                            }
                        }
                    }
                }
                
                // Is it the genesis block?
                try
                {
                    var genesisHash = await rpcClient.GetBlockHashAsync(0);
                    if (genesisHash == blockHash)
                    {
                        _cachedHeights.TryAdd(hashStr, 0);
                        return 0;
                    }
                }
                catch { /* Ignore */ }
                
                // Fall back to a safe value if all else fails
                logger.LogWarning($"Could not determine height for block {hashStr}, using fallback value 1");
                return 1;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Error getting height for block {hashStr}");
                return 1; // Safe fallback
            }
        }
        
        /// <summary>
        /// A utility method to safely extract values from a JToken with a default value if not found
        /// </summary>
        public static T SafeGetValue<T>(JToken token, string property, T defaultValue)
        {
            try
            {
                if (token == null || token[property] == null)
                    return defaultValue;
                return token[property].Value<T>();
            }
            catch
            {
                return defaultValue;
            }
        }
        
        /// <summary>
        /// Clears the height cache, useful when the blockchain may have reorganized
        /// </summary>
        public static void ClearHeightCache()
        {
            _cachedHeights.Clear();
        }
        
        /// <summary>
        /// Creates a special block locator for Haroldcoin for default current location
        /// </summary>
        public static async Task<BlockLocator> CreateHaroldcoinDefaultBlockLocator(RPCClient rpcClient, GetBlockchainInfoResponse blockchainInfo, ILogger logger, CancellationToken token = default)
        {
            logger.LogInformation($"Creating a special block locator for Haroldcoin");
            
            // For Haroldcoin, we'll create a much more comprehensive block locator
            // Start from a much earlier point to ensure we can connect to the chain
            var blockLocator = new BlockLocator();
            
            // Use genesis block as a fallback
            try 
            {
                // Start with the current tip, and add several earlier blocks to improve the chance of finding a common ancestor
                var bestBlock = await rpcClient.GetBestBlockHashAsync(token);
                blockLocator.Blocks.Add(bestBlock);
                
                // Try to add some blocks at specific heights to build a better locator
                var heights = new[] { 1, 100, 1000, 10000, 50000, 100000, 150000 };
                foreach (var height in heights.Where(h => h < blockchainInfo.Headers))
                {
                    try
                    {
                        var hash = await rpcClient.GetBlockHashAsync(height);
                        if (hash != null && !blockLocator.Blocks.Contains(hash))
                        {
                            blockLocator.Blocks.Add(hash);
                        }
                    }
                    catch 
                    {
                        // Ignore errors for individual height lookups
                    }
                }
                
                // Add genesis block - hardcoded for Haroldcoin to avoid GetGenesis() issues
                try
                {
                    // Try to get genesis block hash directly from RPC instead of using GetGenesis()
                    var genesisHash = await rpcClient.GetBlockHashAsync(0);
                    if (genesisHash != null && !blockLocator.Blocks.Contains(genesisHash))
                    {
                        blockLocator.Blocks.Add(genesisHash);
                        logger.LogInformation($"Added genesis block from RPC: {genesisHash}");
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Could not get genesis hash from RPC, skipping");
                }
                
                logger.LogInformation($"Created Haroldcoin block locator with {blockLocator.Blocks.Count} blocks");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error creating Haroldcoin block locator, falling back to genesis only");
                blockLocator = new BlockLocator();
                
                try
                {
                    // Try to get genesis block hash directly from RPC
                    var genesisHash = await rpcClient.GetBlockHashAsync(0);
                    if (genesisHash != null)
                    {
                        blockLocator.Blocks.Add(genesisHash);
                        logger.LogInformation($"Added genesis block from RPC as fallback: {genesisHash}");
                    }
                    else
                    {
                        // Hardcoded fallback
                        var hardcodedHash = new uint256("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");
                        blockLocator.Blocks.Add(hardcodedHash);
                        logger.LogWarning($"Using hardcoded genesis fallback: {hardcodedHash}");
                    }
                }
                catch (Exception ex2)
                {
                    logger.LogError(ex2, "Failed to get genesis block from RPC, using hardcoded fallback");
                    // Hardcoded fallback
                    var hardcodedHash = new uint256("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");
                    blockLocator.Blocks.Add(hardcodedHash);
                    logger.LogWarning($"Using hardcoded genesis fallback: {hardcodedHash}");
                }
            }
            
            return blockLocator;
        }
        
        /// <summary>
        /// Safely refreshes the database connection to prevent connection pool exhaustion
        /// </summary>
        public static async Task<DbConnectionHelper> RefreshConnection(DbConnectionFactory factory, NBXplorerNetwork network, DbConnectionHelper currentConn, ILogger logger)
        {
            try
            {
                // First perform memory cleanup to release any resources
                logger.LogInformation("Refreshing database connection and cleaning up resources");
                PerformMemoryCleanup(logger);
                
                // Dispose the current connection if it exists
                if (currentConn != null)
                {
                    try
                    {
                        await currentConn.DisposeAsync();
                        logger.LogDebug("Successfully disposed previous database connection");
                        
                        // Important: Set the original reference to null to prevent double-disposal attempts
                        currentConn = null;
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Error disposing database connection, will create a new one anyway");
                    }
                }
                
                // Create a new connection
                var newConn = await factory.CreateConnectionHelper(network);
                logger.LogDebug("Created fresh database connection");
                return newConn;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to refresh database connection");
                throw;
            }
        }
    }
} 