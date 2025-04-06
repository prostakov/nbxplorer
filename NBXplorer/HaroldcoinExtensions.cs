using NBitcoin;
using NBitcoin.RPC;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace NBXplorer
{
    /// <summary>
    /// Extension methods and helper classes specific to Haroldcoin integration
    /// </summary>
    public static class HaroldcoinExtensions
    {
        /// <summary>
        /// A simple class that mimics PeerInfo with public setters
        /// </summary>
        private class SimplePeerInfo
        {
            public int Id { get; set; }
            public EndPoint Address { get; set; }
            public string SubVersion { get; set; }
            public List<string> ServicesNames { get; set; } = new List<string>();
            public bool IsWhiteListed { get; set; }
            public string[] Permissions { get; set; } = Array.Empty<string>();
        }

        /// <summary>
        /// A simple class that inherits from PeerInfo and allows setting of properties
        /// </summary>
        private class MockPeerInfo : PeerInfo
        {
            public new int Id { get; set; }
            public new EndPoint Address { get; set; }
            public new string SubVersion { get; set; }
            public new bool IsWhiteListed { get; set; }
            public new string[] ServicesNames { get; set; }
        }

        /// <summary>
        /// Safe method to get peers information for any cryptocurrency, with special handling for Haroldcoin
        /// </summary>
        public static async Task<PeerInfo[]> SafeGetPeersInfoAsync(this RPCClient rpc, CancellationToken cancellationToken = default)
        {
            try
            {
                if (rpc.Network.NetworkSet.CryptoCode == "HRLD")
                {
                    // Custom implementation for Haroldcoin that's more fault-tolerant
                    var result = await rpc.SendCommandAsync("getpeerinfo", cancellationToken);
                    if (result?.Result is JArray array)
                    {
                        var peerList = new List<SimplePeerInfo>();
                        foreach (JObject peer in array)
                        {
                            try
                            {
                                // Basic fields that we need
                                var peerInfo = new SimplePeerInfo
                                {
                                    Id = peer["id"]?.Value<int>() ?? 0,
                                    SubVersion = peer["subver"]?.Value<string>() ?? string.Empty
                                };
                                
                                // Try to parse address if available
                                var addrStr = peer["addr"]?.Value<string>();
                                if (!string.IsNullOrEmpty(addrStr))
                                {
                                    try
                                    {
                                        // Manual parsing of endpoint string in format "ip:port"
                                        var parts = addrStr.Split(':');
                                        if (parts.Length >= 2 && IPAddress.TryParse(parts[0], out var ip))
                                        {
                                            int port = 8333; // Default port
                                            if (int.TryParse(parts[parts.Length - 1], out var parsedPort))
                                                port = parsedPort;
                                            peerInfo.Address = new IPEndPoint(ip, port);
                                        }
                                    }
                                    catch { /* Ignore address parsing failures */ }
                                }
                                
                                // Add services if available
                                if (peer["services"]?.Value<string>() != null)
                                {
                                    // Add a basic "NETWORK" service for compatibility
                                    peerInfo.ServicesNames.Add("NETWORK");
                                }
                                
                                // Add whitelisted status if available
                                if (peer["whitelisted"]?.Value<bool>() == true)
                                {
                                    peerInfo.IsWhiteListed = true;
                                }
                                
                                peerList.Add(peerInfo);
                            }
                            catch 
                            {
                                // Skip problematic peers
                                continue;
                            }
                        }
                        
                        // Create our own mock PeerInfo instances
                        var mockPeerInfos = new List<PeerInfo>();
                        foreach (var simplePeer in peerList)
                        {
                            try 
                            {
                                // Add a minimal implementation that just keeps track of our needed properties
                                mockPeerInfos.Add(new MockPeerInfo 
                                {
                                    Id = simplePeer.Id,
                                    Address = simplePeer.Address,
                                    SubVersion = simplePeer.SubVersion,
                                    IsWhiteListed = simplePeer.IsWhiteListed,
                                    ServicesNames = simplePeer.ServicesNames.ToArray()
                                });
                            }
                            catch
                            {
                                // Skip problematic peers
                            }
                        }

                        return mockPeerInfos.ToArray();
                    }
                    // Return empty array if we couldn't parse the result
                    return Array.Empty<PeerInfo>();
                }
                else
                {
                    // For other cryptocurrencies, use the original method
                    return await rpc.GetPeersInfoAsync(cancellationToken);
                }
            }
            catch
            {
                // If anything fails, return an empty array rather than crashing
                return Array.Empty<PeerInfo>();
            }
        }

        /// <summary>
        /// Safe method to get block header information for any cryptocurrency, with special handling for Haroldcoin
        /// </summary>
        public static async Task<RPCBlockHeader> SafeGetBlockHeaderAsyncEx(this RPCClient rpc, uint256 blk, CancellationToken cancellationToken = default)
        {
            try
            {
                // For Haroldcoin, we need special handling since getblockheader doesn't include height
                if (rpc.Network.NetworkSet.CryptoCode == "HRLD")
                {
                    return await GetHaroldcoinBlockHeader(rpc, blk, cancellationToken);
                }
                
                // Standard approach for other cryptocurrencies
                var header = await rpc.SendCommandAsync(new RPCRequest("getblockheader", new[] { blk.ToString() })
                {
                    ThrowIfRPCError = false
                }, cancellationToken);
                
                if (header == null || header.Result == null || header.Error != null)
                    return null;
                    
                JToken response = header.Result;
                
                // Create a safe method to extract values
                T SafeGetValue<T>(JToken token, string property, T defaultValue)
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
                
                // Extract values safely
                long confs = SafeGetValue<long>(response, "confirmations", 0);
                if (confs == -1)
                    return null;
                    
                string prevHashStr = SafeGetValue<string>(response, "previousblockhash", null);
                uint256 previousBlockHash = prevHashStr != null ? new uint256(prevHashStr) : null;
                
                // Try to get the height from the response
                int height = SafeGetValue<int>(response, "height", -1);
                
                // If height is not provided or is invalid, try to retrieve it directly
                if (height <= 0 && confs > 0)
                {
                    try
                    {
                        // Get current block count
                        var blockCountResponse = await rpc.SendCommandAsync(new RPCRequest("getblockcount", Array.Empty<object>())
                        {
                            ThrowIfRPCError = false
                        }, cancellationToken);
                        
                        if (blockCountResponse != null && blockCountResponse.Result != null)
                        {
                            var currentHeight = blockCountResponse.Result.Value<int>();
                            height = currentHeight - (int)confs + 1; // +1 because current block has 1 confirmation
                        }
                    }
                    catch
                    {
                        // If this fails, we'll keep the height as is
                    }
                }
                
                // If we still don't have a valid height
                if (height <= 0)
                {
                    Console.WriteLine($"WARNING: Block {blk} has invalid height: {height}. Will attempt to use a reasonable value.");
                    
                    // As a last resort, try to get the hash for height 0 to check if this is the genesis block
                    try
                    {
                        var genesisResponse = await rpc.SendCommandAsync(new RPCRequest("getblockhash", new object[] { 0 })
                        {
                            ThrowIfRPCError = false
                        }, cancellationToken);
                        
                        if (genesisResponse != null && genesisResponse.Result != null)
                        {
                            var genesisHash = genesisResponse.Result.Value<string>();
                            if (genesisHash == blk.ToString())
                            {
                                // This is the genesis block
                                height = 0;
                            }
                            else
                            {
                                // Not genesis, use a reasonable value based on previous block if available
                                height = 1; // Default to 1 if we can't figure it out
                            }
                        }
                    }
                    catch
                    {
                        // If all else fails, use 1 as a safer default than 0
                        height = 1;
                    }
                }
                
                long timeUnix = SafeGetValue<long>(response, "time", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                DateTimeOffset time = NBitcoin.Utils.UnixTimeToDateTime(timeUnix);
                
                string merkleRootStr = SafeGetValue<string>(response, "merkleroot", "0000000000000000000000000000000000000000000000000000000000000000");
                uint256 merkleRoot = new uint256(merkleRootStr);
                
                return new RPCBlockHeader(blk, previousBlockHash, height, time, merkleRoot);
            }
            catch
            {
                // If anything fails, return null instead of throwing
                return null;
            }
        }
        
        // Cache for block heights to avoid excessive RPC calls
        private static Dictionary<string, int> _blockHeightCache = new Dictionary<string, int>();
        
        /// <summary>
        /// Special method to get block header information for Haroldcoin, using height tracking
        /// </summary>
        private static async Task<RPCBlockHeader> GetHaroldcoinBlockHeader(RPCClient rpc, uint256 blk, CancellationToken cancellationToken = default)
        {
            try
            {
                // First, try using getblock instead of getblockheader, with verbosity=1
                var blockResponse = await rpc.SendCommandAsync(new RPCRequest("getblock", new object[] { blk.ToString(), 1 })
                {
                    ThrowIfRPCError = false
                }, cancellationToken);
                
                if (blockResponse == null || blockResponse.Result == null || blockResponse.Error != null)
                    return null;
                
                JToken response = blockResponse.Result;
                
                // Create a safe method to extract values
                T SafeGetValue<T>(JToken token, string property, T defaultValue)
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
                
                string prevHashStr = SafeGetValue<string>(response, "previousblockhash", null);
                uint256 previousBlockHash = prevHashStr != null ? new uint256(prevHashStr) : null;
                
                // Calculate height through multiple methods
                int height = -1;
                long confirmations = SafeGetValue<long>(response, "confirmations", 0);
                
                // Method 1: Check if this is the genesis block
                if (previousBlockHash == null)
                {
                    // This is likely the genesis block
                    height = 0;
                    _blockHeightCache[blk.ToString()] = height;
                    Console.WriteLine($"Haroldcoin: Identified genesis block {blk}");
                }
                // Method 2: Calculate from chain tip using confirmations
                else if (confirmations > 0)
                {
                    try
                    {
                        var chainInfo = await rpc.SendCommandAsync(new RPCRequest("getblockchaininfo", Array.Empty<object>())
                        {
                            ThrowIfRPCError = false
                        }, cancellationToken);
                        
                        if (chainInfo != null && chainInfo.Result != null)
                        {
                            var currentHeight = chainInfo.Result["blocks"].Value<int>();
                            height = currentHeight - (int)confirmations + 1;
                            _blockHeightCache[blk.ToString()] = height;
                            Console.WriteLine($"Haroldcoin: Calculated height {height} for block {blk} using confirmations");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error calculating height from chain tip: {ex.Message}");
                    }
                }
                // Method 3: Calculate from previous block's height
                if (height < 0 && previousBlockHash != null)
                {
                    // Check if the previous block's height is in our cache
                    string prevHashString = previousBlockHash.ToString();
                    if (_blockHeightCache.TryGetValue(prevHashString, out int prevHeight))
                    {
                        height = prevHeight + 1;
                        _blockHeightCache[blk.ToString()] = height;
                        Console.WriteLine($"Haroldcoin: Calculated height {height} for block {blk} using previous block");
                    }
                    else
                    {
                        // Try to get the previous block's height
                        try
                        {
                            var prevHeader = await SafeGetBlockHeaderAsyncEx(rpc, previousBlockHash, cancellationToken);
                            if (prevHeader != null && prevHeader.Height >= 0)
                            {
                                height = prevHeader.Height + 1;
                                _blockHeightCache[blk.ToString()] = height;
                                Console.WriteLine($"Haroldcoin: Calculated height {height} for block {blk} using previous block lookup");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error calculating height from previous block: {ex.Message}");
                        }
                    }
                }
                
                // If all methods failed, make an educated guess
                if (height < 0)
                {
                    // As a last resort, if we've seen other blocks, use a value in the middle of the range
                    if (_blockHeightCache.Count > 0)
                    {
                        height = _blockHeightCache.Values.Max() + 1;
                        Console.WriteLine($"Haroldcoin: Using fallback height {height} for block {blk}");
                    }
                    else
                    {
                        // If we haven't seen any blocks yet, this might be early in the chain
                        height = 1; // Safe assumption that's not the genesis block
                        Console.WriteLine($"Haroldcoin: Using default height {height} for block {blk}");
                    }
                    _blockHeightCache[blk.ToString()] = height;
                }
                
                long timeUnix = SafeGetValue<long>(response, "time", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                DateTimeOffset time = NBitcoin.Utils.UnixTimeToDateTime(timeUnix);
                
                string merkleRootStr = SafeGetValue<string>(response, "merkleroot", "0000000000000000000000000000000000000000000000000000000000000000");
                uint256 merkleRoot = new uint256(merkleRootStr);
                
                return new RPCBlockHeader(blk, previousBlockHash, height, time, merkleRoot);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in GetHaroldcoinBlockHeader: {ex.Message}");
                return null;
            }
        }
    }
} 