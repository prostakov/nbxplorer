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
                                    Id = HaroldcoinHelper.SafeGetValue<int>(peer, "id", 0),
                                    SubVersion = HaroldcoinHelper.SafeGetValue<string>(peer, "subver", string.Empty)
                                };
                                
                                // Try to parse address if available
                                var addrStr = HaroldcoinHelper.SafeGetValue<string>(peer, "addr", null);
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
                                if (HaroldcoinHelper.SafeGetValue<string>(peer, "services", null) != null)
                                {
                                    // Add a basic "NETWORK" service for compatibility
                                    peerInfo.ServicesNames.Add("NETWORK");
                                }
                                
                                // Add whitelisted status if available
                                if (HaroldcoinHelper.SafeGetValue<bool>(peer, "whitelisted", false))
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

                // Extract values safely using shared helper
                long confs = HaroldcoinHelper.SafeGetValue<long>(response, "confirmations", 0);
                if (confs == -1)
                    return null;
                    
                string prevHashStr = HaroldcoinHelper.SafeGetValue<string>(response, "previousblockhash", null);
                uint256 previousBlockHash = prevHashStr != null ? new uint256(prevHashStr) : null;
                
                // Try to get the height from the response
                int height = HaroldcoinHelper.SafeGetValue<int>(response, "height", -1);
                
                // If height is not provided or is invalid, try to use available methods
                if (height <= 0)
                {
                    height = await HaroldcoinHelper.GetBlockHeightSafe(rpc, blk, null, cancellationToken);
                }
                
                string merkleRootStr = HaroldcoinHelper.SafeGetValue<string>(response, "merkleroot", null);
                uint256 merkleRoot = merkleRootStr != null ? new uint256(merkleRootStr) : null;
                
                uint nonce = HaroldcoinHelper.SafeGetValue<uint>(response, "nonce", 0);
                uint bits = HaroldcoinHelper.SafeGetValue<uint>(response, "bits", 0);
                uint version = HaroldcoinHelper.SafeGetValue<uint>(response, "version", 0);
                DateTimeOffset medianTime = HaroldcoinHelper.SafeGetValue<DateTimeOffset>(response, "mediantime", DateTimeOffset.MinValue);
                if (medianTime == DateTimeOffset.MinValue)
                {
                    medianTime = HaroldcoinHelper.SafeGetValue<DateTimeOffset>(response, "time", DateTimeOffset.UtcNow);
                }
                
                // Create the RPCBlockHeader with the extracted information
                return new RPCBlockHeader(
                    blk,
                    previousBlockHash,
                    height,
                    medianTime,
                    merkleRoot
                );
            }
            catch (Exception ex)
            {
                // Log the exception and return null
                Console.WriteLine($"Error in SafeGetBlockHeaderAsyncEx: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Special implementation for Haroldcoin that handles the lack of height information
        /// </summary>
        private static async Task<RPCBlockHeader> GetHaroldcoinBlockHeader(RPCClient rpc, uint256 blk, CancellationToken cancellationToken = default)
        {
            try
            {
                // Try to use the full getblock command to ensure maximum data availability
                var result = await rpc.SendCommandAsync(new RPCRequest("getblock", new[] { blk.ToString() })
                {
                    ThrowIfRPCError = false
                }, cancellationToken);
                
                if (result == null || result.Result == null || result.Error != null)
                {
                    // Fall back to getblockheader if getblock fails
                    result = await rpc.SendCommandAsync(new RPCRequest("getblockheader", new[] { blk.ToString() })
                    {
                        ThrowIfRPCError = false
                    }, cancellationToken);
                    
                    if (result == null || result.Result == null || result.Error != null)
                        return null;
                }
                
                JToken response = result.Result;
                
                // Try to get the height - this is the main challenge with Haroldcoin
                int height = await HaroldcoinHelper.GetBlockHeightSafe(rpc, blk, null, cancellationToken);
                
                // Extract the necessary fields to construct an RPCBlockHeader
                string prevHashStr = HaroldcoinHelper.SafeGetValue<string>(response, "previousblockhash", null);
                uint256 previousBlockHash = prevHashStr != null ? new uint256(prevHashStr) : null;
                
                string merkleRootStr = HaroldcoinHelper.SafeGetValue<string>(response, "merkleroot", null);
                uint256 merkleRoot = merkleRootStr != null ? new uint256(merkleRootStr) : null;
                
                // Get the time, defaulting to current time if not available
                DateTimeOffset blockTime = HaroldcoinHelper.SafeGetValue<DateTimeOffset>(response, "time", DateTimeOffset.UtcNow);
                
                // Create the RPCBlockHeader with the information we were able to extract
                return new RPCBlockHeader(
                    blk,
                    previousBlockHash,
                    height,
                    blockTime,
                    merkleRoot
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in GetHaroldcoinBlockHeader: {ex.Message}");
                return null;
            }
        }
    }
} 