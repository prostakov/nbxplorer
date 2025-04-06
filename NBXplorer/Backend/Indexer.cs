using Dapper;
using Microsoft.Extensions.Logging;
using NBitcoin.Protocol.Behaviors;
using NBitcoin.Protocol;
using NBitcoin.RPC;
using NBitcoin;
using NBXplorer.Configuration;
using NBXplorer.Events;
using NBXplorer.Models;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace NBXplorer.Backend
{
	public class Indexer
	{
		public Indexer(
			AddressPoolService addressPoolService,
			ILogger logger,
			NBXplorerNetwork network,
			RPCClient rpcClient,
			Repository repository,
			DbConnectionFactory connectionFactory,
			ExplorerConfiguration explorerConfiguration,
			ChainConfiguration chainConfiguration,
			EventAggregator eventAggregator)
		{
			AddressPoolService = addressPoolService;
			Logger = logger;
			this.network = network;
			RPCClient = rpcClient;
			Repository = repository;
			ConnectionFactory = connectionFactory;
			ExplorerConfiguration = explorerConfiguration;
			ChainConfiguration = chainConfiguration;
			EventAggregator = eventAggregator;
		}
		CancellationTokenSource cts;
		Task _indexerLoop;
		Task _watchdogLoop;

		// This one will check if the indexer is "stuck" and disconnect the node if it is the case
		async Task WatchdogLoop()
		{
			var cancellationToken = cts.Token;
			wait:
			try
			{
				await Task.Delay(TimeSpan.FromMinutes(5.0), cancellationToken);
				var lastBlock = await SeemsStuck(cancellationToken);
				if (lastBlock is null)
					goto wait;
				await Task.Delay(TimeSpan.FromMinutes(2.0), cancellationToken);
				var lastBlock2 = await SeemsStuck(cancellationToken);
				if (lastBlock != lastBlock2)
					goto wait;
				_Connection?.Dispose($"Sync seems stuck after block {lastBlock.Hash} ({lastBlock.Hash}), restarting the connection.");
				goto wait;
			}
			catch when (cts.Token.IsCancellationRequested)
			{
				goto end;
			}
			catch (Exception ex)
			{
				Logger.LogError(ex, "Unhandled exception in the indexer watchdog");
				goto wait;
			}
			end:;
		}

		async Task<SlimChainedBlock> SeemsStuck(CancellationToken cancellationToken)
		{
			if (State is not (BitcoinDWaiterState.NBXplorerSynching or BitcoinDWaiterState.Ready) ||
						lastIndexedBlock is not { } lastBlock ||
						GetConnectedClient() is not RPCClient rpc)
			{
				return null;
			}
			var blockchainInfo = await rpc.GetBlockchainInfoAsyncEx(cancellationToken);
			return blockchainInfo.BestBlockHash != lastBlock.Hash ? lastBlock : null;
		}

		async Task IndexerLoop()
		{
			TimeSpan retryDelay = TimeSpan.FromSeconds(0);
			retry:
			try
			{
				await IndexerLoopCore(cts.Token);
				if (!cts.Token.IsCancellationRequested)
					goto retry;
			}
			catch when (cts.Token.IsCancellationRequested)
			{
			}
			catch (Exception ex)
			{
				Logger.LogError(ex, $"Unhandled exception in the indexer, retrying in {retryDelay.TotalSeconds} seconds");
				try
				{
					await Task.Delay(retryDelay, cts.Token);
				}
				catch { }
				retryDelay += TimeSpan.FromSeconds(5.0);
				retryDelay = TimeSpan.FromTicks(Math.Min(retryDelay.Ticks, TimeSpan.FromMinutes(1.0).Ticks));
				goto retry;
			}
		}

		class Connection : IDisposable
		{
			public Channel<Object> Events;
			public Channel<Block> Blocks;
			public Node Node;
			public Connection(Node node)
			{
				Node = node;
				Events = Channel.CreateUnbounded<object>(new() { AllowSynchronousContinuations = false });
				Blocks = Channel.CreateUnbounded<Block>(new() { AllowSynchronousContinuations = false });
			}
			bool _Disposed = false;

			public void Dispose()
			{
				Dispose(null);
			}
			public void Dispose(string reason)
			{
				if (_Disposed)
					return;
				Node.DisconnectAsync(reason);
				Events.Writer.TryComplete();
				Blocks.Writer.TryComplete();
				_Disposed = true;
			}
		}
		Connection _Connection;
		private async Task IndexerLoopCore(CancellationToken token)
		{
			await ConnectNode(token);
			var connection = _Connection;
			
			// Special handling for Haroldcoin
			if (Network.CryptoCode == "HRLD")
			{
				await HaroldcoinSyncLoop(connection, token);
				return;
			}
			
			await foreach (var item in connection.Events.Reader.ReadAllAsync(token))
			{
				await using var conn = await ConnectionFactory.CreateConnectionHelper(Network);
				if (item is PullBlocks pb)
				{
					var headers = ConsolidatePullBlocks(connection.Events.Reader, pb);
					var slimChainedBlocks = await RPCClient.GetBlockHeadersAsync(headers.Select(b => b.GetHash()).ToList(), token);
					headers = headers.Where(b => slimChainedBlocks.ByHashes.ContainsKey(b.GetHash())).ToList();
					foreach (var batch in headers.Chunk(maxinflight))
					{
						_ = connection.Node.SendMessageAsync(
							new GetDataPayload(
								batch.Select(b => new InventoryVector(connection.Node.AddSupportedOptions(InventoryType.MSG_BLOCK), b.GetHash())
								).ToArray()));
						var remaining = batch.Select(b => b.GetHash()).ToHashSet();
						List<Block> unorderedBlocks = new List<Block>();
						await foreach (var block in connection.Blocks.Reader.ReadAllAsync(token))
						{
							if (!remaining.Remove(block.Header.GetHash()))
								continue;
							if (lastIndexedBlock is null || block.Header.HashPrevBlock == lastIndexedBlock.Hash)
							{
								SlimChainedBlock slimChainedBlock = slimChainedBlocks.ByHashes[block.Header.GetHash()].ToSlimChainedBlock();
								await SaveMatches(conn, block, slimChainedBlock);
							}
							else
							{
								unorderedBlocks.Add(block);
							}
							if (remaining.Count == 0)
							{
								// There are two reasons to receive unordered blocks:
								//   1. There is a fork.
								//   2. Node decides to send headers without asking.
								if (unorderedBlocks.Count > 0)
								{
									// If there is a fork, we should index the unordered blocks
									bool unconfedBlocks = false;
									bool fork = await RPCClient.GetBlockHeaderAsyncEx(lastIndexedBlock.Hash, token) == null;
									foreach (var b in Enumerable.Zip(unorderedBlocks, slimChainedBlocks)
													.Where(b => fork || b.Second.Height > lastIndexedBlock.Height)
													.OrderBy(b => b.Second.Height)
													.ToList())
									{
										var slimBlock = b.Second;
										if (fork && !unconfedBlocks)
										{
											await conn.MakeOrphanFrom(slimBlock.Height);
											unconfedBlocks = true;
										}
										await SaveMatches(conn, b.First, slimBlock.ToSlimChainedBlock());
									}
								}
								break;
							}
						}
						await SaveProgress(conn);
						await UpdateState(connection.Node);
					}
					if (connection.Node.State == NodeState.HandShaked)
						await AskNextHeaders(connection.Node, token);
				}
				if (item is Transaction tx)
				{
					var txs = PullTransactions(connection.Events.Reader, tx);
					await SaveMatches(conn, txs, null, true);
				}
			}
		}

		// Attempt to pull as much non-conflicting transactions as possible in one batch
		private List<Transaction> PullTransactions(ChannelReader<object> reader, Transaction tx)
		{
			List<Transaction> txs = new List<Transaction>();
			HashSet<OutPoint> spent = new HashSet<OutPoint>(tx.Inputs.Capacity);
			bool EnsureNoConflict(Transaction tx)
			{
				foreach (var i in tx.Inputs.Select(i => i.PrevOut))
					if (!spent.Add(i))
						return false;
				return true;
			}
			EnsureNoConflict(tx);
			txs.Add(tx);

			while (reader.TryPeek(out var p) && p is Transaction tx2)
			{
				if (!EnsureNoConflict(tx2))
					break;
				txs.Add(tx2);
				reader.TryRead(out _);
			}
			return txs;
		}

		// We sometimes receive burst of blocks, with some dups.
		// This method will pump as much headers from the channel as possible, removing the dups
		// along the way.
		private IList<BlockHeader> ConsolidatePullBlocks(ChannelReader<object> reader, PullBlocks pb)
		{
			List<PullBlocks> requests = new List<PullBlocks>();
			requests.Add(pb);
			while (reader.TryPeek(out var p) && p is PullBlocks pb2)
			{
				reader.TryRead(out _);
				requests.Add(pb2);
			}

			var headerCount = requests.Select(r => r.headers.Count).Sum();
			HashSet<uint256> blocks = new HashSet<uint256>(headerCount);
			List<BlockHeader> result = new List<BlockHeader>(headerCount);
			foreach (var h in requests.SelectMany(r => r.headers))
			{
				h.PrecomputeHash(false, true);
				if (blocks.Add(h.GetHash()))
					result.Add(h);
			}
			return result;
		}


		private async Task ConnectNode(CancellationToken token)
		{
			State = BitcoinDWaiterState.NotStarted;
			using (var handshakeTimeout = CancellationTokenSource.CreateLinkedTokenSource(token))
			{
				var userAgent = "NBXplorer-" + RandomUtils.GetInt64();
				var nodeParams = new NodeConnectionParameters()
				{
					UserAgent = userAgent,
					ConnectCancellation = handshakeTimeout.Token,
					IsRelay = true
				};
				if (ExplorerConfiguration.SocksEndpoint != null)
				{
					var socks = new SocksSettingsBehavior()
					{
						OnlyForOnionHosts = false,
						SocksEndpoint = ExplorerConfiguration.SocksEndpoint
					};
					if (ExplorerConfiguration.SocksCredentials != null)
						socks.NetworkCredential = ExplorerConfiguration.SocksCredentials;
					nodeParams.TemplateBehaviors.Add(socks);
				}
				var node = await Node.ConnectAsync(network.NBitcoinNetwork, ChainConfiguration.NodeEndpoint, nodeParams);
				Logger.LogInformation($"TCP Connection succeed, handshaking...");
				node.VersionHandshake(handshakeTimeout.Token);
				Logger.LogInformation($"Handshaked");
				await node.SendMessageAsync(new SendHeadersPayload());

				await RPCArgs.TestRPCAsync(Network, RPCClient, token, Logger);
				HasTxIndex = await RPCClient.SupportTxIndex() is true;
				if (HasTxIndex)
				{
					Logger.LogInformation($"Has txindex support");
				}
				var peer = (await RPCClient.SafeGetPeersInfoAsync())
									.FirstOrDefault(p => p.SubVersion == userAgent);
				if (peer.IsWhitelisted())
				{
					if (firstConnect)
					{
						firstConnect = false;
					}
					Logger.LogInformation($"NBXplorer is correctly whitelisted by the node");
				}
				else if (peer is null)
				{
					Logger.LogWarning($"{Network.CryptoCode}: The RPC server you are connecting to, doesn't seem to be the same server as the one providing the P2P connection. This is an untested setup and may have non-obvious side effects.");
				}
				else
				{
					var addressStr = peer.Address is IPEndPoint end ? end.Address.ToString() : peer.Address?.ToString();
					Logger.LogWarning($"{Network.CryptoCode}: Your NBXplorer server is not whitelisted by your node," +
						$" you should add \"whitelist={addressStr}\" to the configuration file of your node. (Or use whitebind)");
				}

				int waitTime = 10;

				// Need NetworkInfo for the get status
				NetworkInfo = await RPCClient.GetNetworkInfoAsync();
				retry:
				BlockchainInfo = await RPCClient.GetBlockchainInfoAsyncEx();
				if (BlockchainInfo.IsSynching(Network))
				{
					State = BitcoinDWaiterState.CoreSynching;
					await Task.Delay(waitTime * 2, token);
					waitTime = Math.Min(5_000, waitTime * 2);
					goto retry;
				}
				
				// Disable creation of wallet
				// await RPCClient.EnsureWalletCreated(Logger);
				Logger.LogInformation("Skipping creation of wallet");
				
				if (Network.NBitcoinNetwork.ChainName == ChainName.Regtest && !ChainConfiguration.NoWarmup)
				{
					if (await RPCClient.WarmupBlockchain(Logger))
						BlockchainInfo = await RPCClient.GetBlockchainInfoAsyncEx();
				}
				_NodeTip = (await RPCClient.GetBlockHeaderAsyncEx(BlockchainInfo.BestBlockHash, token))?.ToSlimChainedBlock();
				State = BitcoinDWaiterState.NBXplorerSynching;
				// Refresh the NetworkInfo that may have become different while it was synching.
				NetworkInfo = await RPCClient.GetNetworkInfoAsync();

				_Connection?.Dispose("Creating new connection");
				_Connection = new Connection(node);
				node.MessageReceived += Node_MessageReceived;
				node.Disconnected += Node_Disconnected;
				var locator = await AskNextHeaders(node, token);
				lastIndexedBlock = await Repository.GetLastIndexedSlimChainedBlock(locator);
				if (lastIndexedBlock is null)
				{
					var locatorTip = await RPCClient.GetBlockHeaderAsyncEx(locator.Blocks[0], token);
					lastIndexedBlock = locatorTip?.ToSlimChainedBlock();
				}
				await UpdateState(node);
			}
		}

		bool firstConnect = true;
		private async Task<BlockLocator> AskNextHeaders(Node node, CancellationToken token)
		{
			var indexProgress = await Repository.GetIndexProgress();
			if (indexProgress is null)
			{
				indexProgress = await GetDefaultCurrentLocation(token);
			}
			
			// Log the locator blocks we're using to request headers
			if (indexProgress?.Blocks?.Count > 0)
			{
				Logger.LogInformation($"{Network.CryptoCode}: Requesting headers from block {indexProgress.Blocks[0]} (locator has {indexProgress.Blocks.Count} blocks)");
			}
			else
			{
				Logger.LogWarning($"{Network.CryptoCode}: Requesting headers with empty locator");
			}
			
			// For Haroldcoin, add a specific timeout and retry mechanism
			if (Network.CryptoCode == "HRLD")
			{
				// Send a more aggressive combination of messages to improve chances of response
				try
				{
					// Try with empty locator first
					if (indexProgress.Blocks.Count == 0 || BlockchainInfo.Headers > 1000)
					{
						Logger.LogInformation($"HRLD: Trying GetHeaders with empty locator");
						await node.SendMessageAsync(new GetHeadersPayload());
						await Task.Delay(200, token);
					}
					
					// Then try with standard locator
					Logger.LogInformation($"HRLD: Sending GetHeadersPayload with {indexProgress.Blocks.Count} blocks");
					await node.SendMessageAsync(new GetHeadersPayload(indexProgress));
					await Task.Delay(200, token);
					
					// Also try from genesis
					try 
					{
						// Try to get genesis block hash through RPC
						var genesisHash = await RPCClient.GetBlockHashAsync(0);
						if (genesisHash != null)
						{
							var genesisLocator = new BlockLocator();
							genesisLocator.Blocks.Add(genesisHash);
							Logger.LogInformation($"HRLD: Sending GetHeadersPayload from genesis: {genesisHash}");
							await node.SendMessageAsync(new GetHeadersPayload(genesisLocator));
							await Task.Delay(200, token);
						}
					}
					catch (Exception gex)
					{
						Logger.LogError(gex, "Error sending genesis headers request");
					}
					
					// Try GetBlocks as well
					Logger.LogInformation($"HRLD: Sending GetBlocksPayload");
					await node.SendMessageAsync(new GetBlocksPayload(indexProgress));
					
					// Send a ping to keep the connection alive and check status
					await Task.Delay(200, token);
					await node.SendMessageAsync(new PingPayload());
				}
				catch (Exception ex)
				{
					Logger.LogError(ex, "Error sending header requests for Haroldcoin");
				}
			}
			else 
			{
				await node.SendMessageAsync(new GetHeadersPayload(indexProgress));
			}
			
			return indexProgress;
		}

		static int[] BlockLocatorComposition = new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, 10240, 20480, 40960 };
		private async Task SaveProgress(DbConnectionHelper conn)
		{
			// We pick blocks spaced exponentially from the the tip to build our block locator
			var heights = BlockLocatorComposition.Select(l => lastIndexedBlock.Height - l).ToArray();
			var blks = await conn.Connection.QueryAsync<string>(
			"SELECT blk_id FROM blks " +
			"WHERE code=@code AND height=ANY(@heights) AND confirmed IS TRUE " +
			"ORDER BY height DESC", new { code = Network.CryptoCode, heights });
			var locator = new BlockLocator();
			foreach (var b in blks)
				locator.Blocks.Add(uint256.Parse(b));
			await Repository.SetIndexProgress(conn.Connection, locator);
		}

		private async Task UpdateState(Node node)
		{
			if (node.State != NodeState.HandShaked)
				return;
			var blockchainInfo = await RPCClient.GetBlockchainInfoAsyncEx();
			if (blockchainInfo.IsSynching(Network))
			{
				State = BitcoinDWaiterState.CoreSynching;
			}
			else if (lastIndexedBlock != null)
			{
				int minBlock = 6;
				// Prevent some corner cases in tests, if we suddenly mine 200 blocks, we should still be synched on regtest
				if (Network.NBitcoinNetwork.ChainName == ChainName.Regtest)
					minBlock = 200;
				State = blockchainInfo.Headers - lastIndexedBlock.Height < minBlock ? BitcoinDWaiterState.Ready : BitcoinDWaiterState.NBXplorerSynching;
			}
		}

		// Version of UpdateState that doesn't require a Node
		private async Task UpdateStateWithoutNode()
		{
			var blockchainInfo = await RPCClient.GetBlockchainInfoAsyncEx();
			if (blockchainInfo.IsSynching(Network))
			{
				State = BitcoinDWaiterState.CoreSynching;
			}
			else if (lastIndexedBlock != null)
			{
				int minBlock = 6;
				// Prevent some corner cases in tests, if we suddenly mine 200 blocks, we should still be synched on regtest
				if (Network.NBitcoinNetwork.ChainName == ChainName.Regtest)
					minBlock = 200;
				State = blockchainInfo.Headers - lastIndexedBlock.Height < minBlock ? BitcoinDWaiterState.Ready : BitcoinDWaiterState.NBXplorerSynching;
			}
		}

		private async Task<BlockLocator> GetDefaultCurrentLocation(CancellationToken token)
		{
			if (ChainConfiguration.StartHeight > BlockchainInfo.Headers)
				throw new InvalidOperationException($"{Network.CryptoCode}: StartHeight ({ChainConfiguration.StartHeight}) should not be above the current tip ({BlockchainInfo.Headers})");
				
			BlockLocator blockLocator = null;
			
			// Special handling for Haroldcoin to improve sync starting point
			if (Network.CryptoCode == "HRLD")
			{
				Logger.LogInformation($"Creating a special block locator for Haroldcoin");
				
				// For Haroldcoin, we'll create a much more comprehensive block locator
				// Start from a much earlier point to ensure we can connect to the chain
				blockLocator = new BlockLocator();
				
				// Use genesis block as a fallback
				try 
				{
					// Start with the current tip, and add several earlier blocks to improve the chance of finding a common ancestor
					var bestBlock = await RPCClient.GetBestBlockHashAsync(token);
					blockLocator.Blocks.Add(bestBlock);
					
					// Try to add some blocks at specific heights to build a better locator
					var heights = new[] { 1, 100, 1000, 10000, 50000, 100000, 150000 };
					foreach (var height in heights.Where(h => h < BlockchainInfo.Headers))
					{
						try
						{
							var hash = await RPCClient.GetBlockHashAsync(height);
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
						var genesisHash = await RPCClient.GetBlockHashAsync(0);
						if (genesisHash != null && !blockLocator.Blocks.Contains(genesisHash))
						{
							blockLocator.Blocks.Add(genesisHash);
							Logger.LogInformation($"Added genesis block from RPC: {genesisHash}");
						}
					}
					catch (Exception ex)
					{
						Logger.LogWarning(ex, "Could not get genesis hash from RPC, skipping");
					}
					
					Logger.LogInformation($"Created Haroldcoin block locator with {blockLocator.Blocks.Count} blocks");
				}
				catch (Exception ex)
				{
					Logger.LogError(ex, "Error creating Haroldcoin block locator, falling back to genesis only");
					blockLocator = new BlockLocator();
					
					try
					{
						// Try to get genesis block hash directly from RPC
						var genesisHash = await RPCClient.GetBlockHashAsync(0);
						if (genesisHash != null)
						{
							blockLocator.Blocks.Add(genesisHash);
							Logger.LogInformation($"Added genesis block from RPC as fallback: {genesisHash}");
						}
						else
						{
							// Hardcoded fallback
							var hardcodedHash = new uint256("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");
							blockLocator.Blocks.Add(hardcodedHash);
							Logger.LogWarning($"Using hardcoded genesis fallback: {hardcodedHash}");
						}
					}
					catch (Exception ex2)
					{
						Logger.LogError(ex2, "Failed to get genesis block from RPC, using hardcoded fallback");
						// Hardcoded fallback
						var hardcodedHash = new uint256("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");
						blockLocator.Blocks.Add(hardcodedHash);
						Logger.LogWarning($"Using hardcoded genesis fallback: {hardcodedHash}");
					}
				}
				
				return blockLocator;
			}
			
			if (ChainConfiguration.StartHeight == -1)
			{
				var bestBlock = await RPCClient.GetBestBlockHashAsync(token);
				var bh = await RPCClient.GetBlockHeaderAsyncEx(bestBlock, token);
				blockLocator = new BlockLocator();
				blockLocator.Blocks.Add(bh.Previous ?? bh.Hash);
				Logger.LogInformation($"Current Index Progress not found, start syncing from the header's chain tip (At height: {BlockchainInfo.Headers})");
			}
			else
			{
				var header = await RPCClient.GetBlockHeaderAsync(ChainConfiguration.StartHeight, token);
				var header2 = await RPCClient.GetBlockHeaderAsyncEx(header.GetHash(), token);
				blockLocator = new BlockLocator();
				blockLocator.Blocks.Add(header2.Previous ?? header2.Hash);
				Logger.LogInformation($"Current Index Progress not found, start syncing at height {ChainConfiguration.StartHeight}");
			}
			return blockLocator;
		}

		private async Task SaveMatches(DbConnectionHelper conn, Block block, SlimChainedBlock slimChainedBlock)
		{
			block.Header.PrecomputeHash(false, false);
			// If we are synching, the block time is better approximation of the received time
			var seenAt = State == BitcoinDWaiterState.NBXplorerSynching
							? block.Header.BlockTime
							: DateTimeOffset.UtcNow;
			await SaveMatches(conn, block.Transactions, slimChainedBlock, true, seenAt);
			EventAggregator.Publish(new RawBlockEvent(block, this.Network), true);
			lastIndexedBlock = slimChainedBlock;
		}

		SlimChainedBlock _NodeTip;

		private async Task SaveMatches(DbConnectionHelper conn, List<Transaction> transactions, SlimChainedBlock slimChainedBlock, bool fireEvents, DateTimeOffset? seenAt = null)
		{
			foreach (var tx in transactions)
				tx.PrecomputeHash(false, true);
			var now = seenAt ?? DateTimeOffset.UtcNow;
			if (slimChainedBlock != null)
			{
				await conn.NewBlock(slimChainedBlock);
			}
			var matches = await Repository.GetMatches(conn, transactions, slimChainedBlock, now, useCache: true, cancellationToken: cts.Token);
			_ = AddressPoolService.GenerateAddresses(Network, matches);

			long confirmations = 0;
			if (slimChainedBlock != null)
			{
				if (slimChainedBlock.Height >= _NodeTip.Height)
					_NodeTip = slimChainedBlock;
				confirmations = _NodeTip.Height - slimChainedBlock.Height + 1;
				await conn.NewBlockCommit(slimChainedBlock.Hash);
				var blockEvent = new Models.NewBlockEvent()
				{
					CryptoCode = Network.CryptoCode,
					Hash = slimChainedBlock.Hash,
					Height = slimChainedBlock.Height,
					PreviousBlockHash = slimChainedBlock.Previous,
					Confirmations = confirmations
				};
				await Repository.SaveEvent(conn, blockEvent);
				EventAggregator.Publish(blockEvent);
			}
			if (fireEvents)
			{
				NewTransactionEvent[] evts = new NewTransactionEvent[matches.Length];
				for (int i = 0; i < matches.Length; i++)
				{
					var txEvt = new Models.NewTransactionEvent()
					{
						TrackedSource = matches[i].TrackedSource,
						DerivationStrategy = (matches[i].TrackedSource is DerivationSchemeTrackedSource dsts) ? dsts.DerivationStrategy : null,
						CryptoCode = Network.CryptoCode,
						BlockId = slimChainedBlock?.Hash,
						TransactionData = new TransactionResult()
						{
							BlockId = slimChainedBlock?.Hash,
							Height = slimChainedBlock?.Height,
							Confirmations = confirmations,
							Timestamp = now,
							Transaction = matches[i].Transaction,
							TransactionHash = matches[i].TransactionHash,
							Metadata = matches[i].Metadata
						},
						Inputs = matches[i].MatchedInputs,
						Outputs = matches[i].MatchedOutputs,
						Replacing = matches[i].Replacing.ToList()
					};

					evts[i] = txEvt;
				}
				await Repository.SaveEvents(conn, evts);
				foreach (var ev in evts)
				{
					EventAggregator.Publish(ev);
				}
			}
		}

		SlimChainedBlock lastIndexedBlock;
		record PullBlocks(IList<BlockHeader> headers);
		private void Node_MessageReceived(Node node, IncomingMessage message)
		{
			var connection = _Connection;
			Logger.LogDebug($"{Network.CryptoCode}: Received message type: {message.Message.Payload.GetType().Name}");
			
			if (message.Message.Payload is HeadersPayload h && h.Headers.Count != 0)
			{
				Logger.LogDebug($"{Network.CryptoCode}: Received {h.Headers.Count} headers");
				connection.Events.Writer.TryWrite(new PullBlocks(h.Headers));
			}
			else if (message.Message.Payload is BlockPayload b)
			{
				Logger.LogDebug($"{Network.CryptoCode}: Received block {b.Object.GetHash()}");
				connection.Blocks.Writer.TryWrite(b.Object);
			}
			else if (message.Message.Payload is InvPayload invs)
			{
				Logger.LogDebug($"{Network.CryptoCode}: Received inv with {invs.Inventory.Count} items");
				if (State != BitcoinDWaiterState.Ready)
					return;
				var data = new GetDataPayload();
				foreach (var inv in invs.Inventory.Where(t => t.Type.HasFlag(InventoryType.MSG_TX)))
				{
					inv.Type = node.AddSupportedOptions(inv.Type);
					data.Inventory.Add(inv);
				}
				if (data.Inventory.Count != 0)
				{
					node.SendMessageAsync(data);
				}
			}
			else if (message.Message.Payload is TxPayload tx)
			{
				Logger.LogDebug($"{Network.CryptoCode}: Received transaction {tx.Object.GetHash()}");
				connection.Events.Writer.TryWrite(tx.Object);
			}
		}

		private void Node_Disconnected(Node node)
		{
			Logger.LogInformation($"Node disconnected ({node.DisconnectReason.Reason})");
			_Connection?.Dispose();
			node.MessageReceived -= Node_MessageReceived;
			node.Disconnected -= Node_Disconnected;
			State = BitcoinDWaiterState.NotStarted;
		}


		public async Task StartAsync(CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested)
				return;
			await Task.Yield(); // So it doesn't crash the calling Task.WhenAll
			cts = new CancellationTokenSource();
			_indexerLoop = IndexerLoop();
			_watchdogLoop = WatchdogLoop();
		}

		public async Task StopAsync(CancellationToken cancellationToken)
		{
			cts?.Cancel();
			_Connection?.Dispose("NBXplorer stopping...");
			if (_indexerLoop is not null)
				await _indexerLoop;
			if (_watchdogLoop is not null)
				await _watchdogLoop;
		}
		public NBXplorerNetwork Network => network;

		BitcoinDWaiterState _State = BitcoinDWaiterState.NotStarted;
		public BitcoinDWaiterState State
		{
			get
			{
				return _State;
			}
			set
			{
				if (_State != value)
				{
					var old = _State;
					_State = value;
					EventAggregator.Publish(new BitcoinDStateChangedEvent(Network, old, value));
				}
			}
		}

		public long? SyncHeight => lastIndexedBlock?.Height;

		public GetNetworkInfoResponse NetworkInfo { get; internal set; }
		public AddressPoolService AddressPoolService { get; }
		public ILogger Logger { get; }
		public RPCClient RPCClient { get; }
		public Repository Repository { get; }
		public DbConnectionFactory ConnectionFactory { get; }
		public ExplorerConfiguration ExplorerConfiguration { get; }
		public ChainConfiguration ChainConfiguration { get; }
		public EventAggregator EventAggregator { get; }
		public GetBlockchainInfoResponse BlockchainInfo { get; private set; }
		public bool HasTxIndex { get; private set; }

		NBXplorerNetwork network;
		private int maxinflight = 10;

		public async Task SaveMatches(Transaction transaction)
		{
			await using var conn = await ConnectionFactory.CreateConnectionHelper(Network);
			await SaveMatches(conn, new List<Transaction>(1) { transaction }, null, false);
		}

		public RPCClient GetConnectedClient() => State switch
		{
			BitcoinDWaiterState.CoreSynching or BitcoinDWaiterState.NBXplorerSynching or BitcoinDWaiterState.Ready => RPCClient,
			_ => null
		};

		// Specialized loop for Haroldcoin to handle its P2P protocol differences
		private async Task HaroldcoinSyncLoop(Connection connection, CancellationToken token)
		{
			Logger.LogInformation($"Starting specialized Haroldcoin synchronization loop");
			
			// Create a persistent connection to handle DB operations
			await using var dbConn = await ConnectionFactory.CreateConnectionHelper(Network);
			
			// Get the current blockchain info to start with
			var blockchainInfo = await RPCClient.GetBlockchainInfoAsyncEx();
			var startHeight = lastIndexedBlock?.Height ?? 0;
			var targetHeight = blockchainInfo.Headers;
			
			Logger.LogInformation($"Haroldcoin: Syncing from height {startHeight} to {targetHeight}");
			
			// Skip P2P sync attempts and go directly to RPC sync for Haroldcoin
			Logger.LogInformation("Haroldcoin: Using direct RPC sync for more reliable synchronization");
			await FallbackToDirectSync(dbConn, token);
			return;
		}
		
		private async Task FallbackToDirectSync(DbConnectionHelper dbConn, CancellationToken token)
		{
			try
			{
				// Get current blockchain info
				var blockchainInfo = await RPCClient.GetBlockchainInfoAsyncEx();
				var startHeight = lastIndexedBlock?.Height ?? 0;
				var endHeight = blockchainInfo.Headers;
				
				Logger.LogInformation($"Haroldcoin: Direct sync from height {startHeight} to {endHeight}");
				
				// Check for blocks with height 0 (potential issue)
				await FixBlockHeightsInDatabase(dbConn);
				
				// IMPORTANT: Initialize _NodeTip if it's null to prevent NullReferenceException in SaveMatches
				if (_NodeTip == null)
				{
					// Get the best block hash and create a SlimChainedBlock for it
					var bestBlockHash = await RPCClient.GetBestBlockHashAsync();
					_NodeTip = new SlimChainedBlock(bestBlockHash, uint256.Zero, (int)endHeight);
					Logger.LogInformation($"Initialized _NodeTip with height {endHeight} and hash {bestBlockHash}");
				}
				
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
								Logger.LogWarning($"Haroldcoin: Retry #{retryCount} for block at height {height}");
								await Task.Delay(retryDelay * retryCount, token);
							}
							
							// Get the block hash at this height - critical step
							var hash = await RPCClient.GetBlockHashAsync(height);
							if (hash == null)
							{
								Logger.LogWarning($"Haroldcoin: Could not get hash for block at height {height}");
								continue; // Try again if we have retries left
							}
							
							// Log progress every 100 blocks
							if (height % 100 == 0 || height == (int)startHeight + 1 || height == endHeight)
							{
								double progressPct = (height - startHeight) * 100.0 / (endHeight - startHeight);
								Logger.LogInformation($"Haroldcoin: Syncing block {height}/{endHeight} - {progressPct:F2}% complete");
							}
							
							// Get the block - critical step
							var block = await RPCClient.GetBlockAsync(hash);
							if (block == null)
							{
								Logger.LogWarning($"Haroldcoin: Could not get block at height {height} (hash: {hash})");
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
							
							Logger.LogInformation($"Haroldcoin: Created header for block at height {height} (hash: {hash}, prev: {previousBlockHash})");
							
							// Save the block
							await SaveMatches(dbConn, block, slimChainedBlock);
							
							// Mark as successfully processed
							blockProcessed = true;
							
							// Save progress periodically
							if (height % 10 == 0)
							{
								await SaveProgress(dbConn);
								await UpdateStateWithoutNode();
							}
						}
						catch (Exception ex)
						{
							Logger.LogError(ex, $"Error processing block at height {height}, retry {retryCount+1}/{maxRetries}");
							
							// If this was our last retry, and it's still failing
							if (retryCount == maxRetries - 1)
							{
								// This is important: we need to decide whether to continue or abort the sync
								// For now, we'll just log and continue to the next height
								Logger.LogWarning($"Haroldcoin: Failed to process block at height {height} after {maxRetries} attempts, skipping to next height");
								
								// Add a slightly longer delay before moving to the next block
								await Task.Delay(retryDelay * 2, token);
							}
						}
					}
				}
				
				// Final save of progress
				await SaveProgress(dbConn);
				await UpdateStateWithoutNode();
				
				// Fix any remaining blocks with incorrect heights
				await FixBlockHeightsInDatabase(dbConn);
				
				Logger.LogInformation($"Haroldcoin: Direct sync completed to height {endHeight}");
				
				// Add an extra consistency check for blocks that might have been missed
				await CheckForMissingBlocks(dbConn, (int)startHeight, (int)endHeight);
			}
			catch (Exception ex)
			{
				Logger.LogError(ex, "Error in direct sync");
				throw;
			}
		}
		
		/// <summary>
		/// Checks for missing blocks in the height range and attempts to retrieve them
		/// </summary>
		private async Task CheckForMissingBlocks(DbConnectionHelper dbConn, int startHeight, int endHeight)
		{
			try
			{
				Logger.LogInformation($"Checking for missing blocks in range {startHeight}-{endHeight}...");
				
				// Get all heights in the database for our crypto code
				var existingHeights = (await dbConn.Connection.QueryAsync<int>(
					"SELECT height FROM blks WHERE code=@code AND height BETWEEN @startHeight AND @endHeight ORDER BY height",
					new { code = Network.CryptoCode, startHeight, endHeight })).ToHashSet();
				
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
					Logger.LogInformation("No missing blocks found in the database.");
					return;
				}
				
				Logger.LogWarning($"Found {missingHeights.Count} missing blocks in the database. Attempting to retrieve them...");
				
				// Process missing blocks
				int processedCount = 0;
				foreach (var height in missingHeights)
				{
					try
					{
						// Get the block hash
						var hash = await RPCClient.GetBlockHashAsync(height);
						if (hash == null)
						{
							Logger.LogWarning($"Could not get hash for missing block at height {height}");
							continue;
						}
						
						// Get the block
						var block = await RPCClient.GetBlockAsync(hash);
						if (block == null)
						{
							Logger.LogWarning($"Could not get missing block at height {height} (hash: {hash})");
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
						await SaveMatches(dbConn, block, slimChainedBlock);
						processedCount++;
						
						Logger.LogInformation($"Successfully processed missing block at height {height} (hash: {hash})");
						
						// Save progress periodically
						if (processedCount % 10 == 0)
						{
							await SaveProgress(dbConn);
							await UpdateStateWithoutNode();
						}
					}
					catch (Exception ex)
					{
						Logger.LogError(ex, $"Error processing missing block at height {height}");
					}
				}
				
				// Final save after processing missing blocks
				if (processedCount > 0)
				{
					await SaveProgress(dbConn);
					await UpdateStateWithoutNode();
					Logger.LogInformation($"Successfully processed {processedCount} missing blocks");
				}
			}
			catch (Exception ex)
			{
				Logger.LogError(ex, "Error checking for missing blocks");
			}
		}
		
		/// <summary>
		/// Checks for blocks with height 0 in the database and corrects them based on their position in the chain
		/// </summary>
		private async Task FixBlockHeightsInDatabase(DbConnectionHelper dbConn)
		{
			if (Network.CryptoCode != "HRLD")
				return;
				
			try
			{
				Logger.LogInformation("Checking for blocks with incorrect heights in database...");
				
				// Query to find blocks with height 0 (except genesis block)
				var blocksWithZeroHeight = await dbConn.Connection.QueryAsync<(string blk_id, string prev_id)>(
					"SELECT blk_id, prev_id FROM blks WHERE code=@code AND height=0 AND prev_id IS NOT NULL",
					new { code = Network.CryptoCode });
					
				var blocks = blocksWithZeroHeight.ToList();
				if (blocks.Count == 0)
				{
					Logger.LogInformation("No blocks with incorrect heights found.");
					return;
				}
				
				Logger.LogWarning($"Found {blocks.Count} blocks with height 0 that need fixing");
				
				// Create a lookup by previous hash
				var blocksByPrev = blocks.ToDictionary(b => b.prev_id, b => b.blk_id);
				
				// Get genesis block
				var genesisHash = await RPCClient.GetBlockHashAsync(0);
				if (genesisHash == null)
				{
					Logger.LogWarning("Could not get genesis block hash");
					return;
				}
				
				// Get the current height from the node
				var blockchainInfo = await RPCClient.GetBlockchainInfoAsyncEx();
				
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
							new { code = Network.CryptoCode, blk_id = nextHash, height });
							
						currentHash = nextHash;
						fixedCount++;
						
						if (fixedCount % 100 == 0)
						{
							Logger.LogInformation($"Fixed {fixedCount} block heights so far");
						}
					}
					catch (Exception ex)
					{
						Logger.LogError(ex, $"Error fixing height for block {nextHash}");
						break;
					}
				}
				
				Logger.LogInformation($"Fixed heights for {fixedCount} blocks");
				
				// One more check for any blocks missed by the chain traversal
				var remainingZeroHeights = await dbConn.Connection.QueryAsync<int>(
					"SELECT COUNT(*) FROM blks WHERE code=@code AND height=0 AND prev_id IS NOT NULL",
					new { code = Network.CryptoCode });
					
				if (remainingZeroHeights.First() > 0)
				{
					Logger.LogWarning($"There are still {remainingZeroHeights.First()} blocks with height 0 that couldn't be fixed automatically");
				}
			}
			catch (Exception ex)
			{
				Logger.LogError(ex, "Error checking for blocks with incorrect heights");
			}
		}
	}
}
