using System;
using NBitcoin;
using NBitcoin.Protocol;

namespace NBXplorer.Client;

public class Haroldcoin : NetworkSetBase
{
    public static Haroldcoin Instance { get; } = new Haroldcoin();

    public override string CryptoCode => "HRLD";

    private Haroldcoin()
    {
    }

    public class HaroldcoinConsensusFactory : ConsensusFactory
    {
        public static HaroldcoinConsensusFactory Instance = new HaroldcoinConsensusFactory();

        public override Transaction CreateTransaction()
        {
            return new HaroldcoinTransaction();
        }

        public override Block CreateBlock()
        {
            return new HaroldcoinBlock();
        }

        public override BlockHeader CreateBlockHeader()
        {
            return new HaroldcoinBlockHeader();
        }

        public override TxOut CreateTxOut()
        {
            return new HaroldcoinTxOut();
        }
    }

    public class HaroldcoinTxOut : TxOut
    {
        public override Money GetDustThreshold()
        {
            return Money.Coins(0.00001m);
        }

        public override ConsensusFactory GetConsensusFactory()
        {
            return HaroldcoinConsensusFactory.Instance;
        }
    }

    public class HaroldcoinTransaction : Transaction
    {
        public override ConsensusFactory GetConsensusFactory()
        {
            return HaroldcoinConsensusFactory.Instance;
        }
    }

    public class HaroldcoinBlockHeader : BlockHeader
    {
        public override uint256 GetPoWHash()
        {
            var headerBytes = this.ToBytes();
            var h = NBitcoin.Crypto.SCrypt.ComputeDerivedKey(headerBytes, headerBytes, 1024, 1, 1, null, 32);
            return new uint256(h);
        }
    }

    public class HaroldcoinBlock : Block
    {
        public override ConsensusFactory GetConsensusFactory()
        {
            return Haroldcoin.Instance.Mainnet.Consensus.ConsensusFactory;
        }
    }

    protected override void PostInit()
    {
        RegisterDefaultCookiePath("Haroldcoin");
    }

    protected override NetworkBuilder CreateMainnet()
    {
	    // TODO: Add haroldcoin-specific implementation
	    throw new NotImplementedException();
    }

    protected override NetworkBuilder CreateTestnet()
    {
	    // TODO: Add haroldcoin-specific implementation
	    throw new NotImplementedException();
    }

    protected override NetworkBuilder CreateRegtest()
    {
	    // TODO: Add haroldcoin-specific implementation
	    throw new NotImplementedException();
    }
}