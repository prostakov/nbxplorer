using NBitcoin;
using NBitcoin.RPC;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;
using System;
using NBitcoin.Protocol;

namespace NBXplorer
{
    public static class HaroldcoinHelper
    {
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
                // Hardcoded fallback for Haroldcoin genesis (you should replace this with the actual genesis hash)
                var hardcodedHash = new uint256("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");
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
    }
} 