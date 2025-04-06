using NBitcoin;
using NBitcoin.RPC;
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
                
                int height = SafeGetValue<int>(response, "height", 0);
                
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
    }
} 