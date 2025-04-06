using NBitcoin;
using NBXplorer.Client;

namespace NBXplorer;

public partial class NBXplorerNetworkProvider
{
	private void InitHaroldcoin(ChainName networkType)
	{
		Add(new NBXplorerNetwork(Haroldcoin.Instance, networkType)
		{
			// TODO: Adjust to haroldcoin needs
			// MinRPCVersion = 70910,
			// CoinType = networkType == ChainName.Mainnet ? new KeyPath("35104'") : new KeyPath("1'")
		});
	}
}