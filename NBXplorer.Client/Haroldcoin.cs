using System;
using NBitcoin;
using NBitcoin.DataEncoders;
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
        var builder = new NetworkBuilder();
        builder.SetConsensus(new Consensus()
        {
            SubsidyHalvingInterval = 0,
            MajorityEnforceBlockUpgrade = 750,
            MajorityRejectBlockOutdated = 950,
            MajorityWindow = 1000,
            BIP34Hash = new uint256("0x00000f2fddbb7212e6f36f398461e8ad49dba752608c9c7322cb97e9a893b485"),
            PowLimit = new Target(new uint256("00000fffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")),
            PowTargetTimespan = TimeSpan.FromSeconds(2 * 60),
            PowTargetSpacing = TimeSpan.FromSeconds(10 * 60),
            PowAllowMinDifficultyBlocks = false,
            PowNoRetargeting = false,
            RuleChangeActivationThreshold = 1916,
            MinerConfirmationWindow = 2016,
            CoinbaseMaturity = 10,
            ConsensusFactory = HaroldcoinConsensusFactory.Instance
        })
        .SetBase58Bytes(Base58Type.PUBKEY_ADDRESS, new byte[] { 40 })
        .SetBase58Bytes(Base58Type.SCRIPT_ADDRESS, new byte[] { 41 })
        .SetBase58Bytes(Base58Type.SECRET_KEY, new byte[] { 126 })
        .SetBase58Bytes(Base58Type.EXT_PUBLIC_KEY, new byte[] { 0x02, 0x02, 0x21, 0x33 })
        .SetBase58Bytes(Base58Type.EXT_SECRET_KEY, new byte[] { 0x2D, 0x21, 0x4C, 0x2B })
        .SetBech32(Bech32Type.WITNESS_PUBKEY_ADDRESS, Encoders.Bech32("hrld"))
        .SetBech32(Bech32Type.WITNESS_SCRIPT_ADDRESS, Encoders.Bech32("hrld"))
        .SetMagic(0xe82b65ad)
        .SetPort(25676)
        .SetRPCPort(25674)
        .SetName("haroldcoin-main")
        .AddAlias("haroldcoin-mainnet")
        .AddDNSSeeds(new[]
        {
            new DNSSeedData("64.227.126.212", "64.227.126.212")
        })
        .AddSeeds(new NetworkAddress[0])
        .SetGenesis("00000f2fddbb7212e6f36f398461e8ad49dba752608c9c7322cb97e9a893b485");

        return builder;
    }

    protected override NetworkBuilder CreateTestnet()
    {
        var builder = new NetworkBuilder();
        builder.SetConsensus(new Consensus()
        {
            SubsidyHalvingInterval = 0,
            MajorityEnforceBlockUpgrade = 51,
            MajorityRejectBlockOutdated = 75,
            MajorityWindow = 100,
            PowLimit = new Target(new uint256("00000fffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")),
            PowTargetTimespan = TimeSpan.FromSeconds(1 * 30),
            PowTargetSpacing = TimeSpan.FromSeconds(5 * 60),
            PowAllowMinDifficultyBlocks = true,
            PowNoRetargeting = false,
            RuleChangeActivationThreshold = 1,
            MinerConfirmationWindow = 2,
            CoinbaseMaturity = 15,
            ConsensusFactory = HaroldcoinConsensusFactory.Instance
        })
        .SetBase58Bytes(Base58Type.PUBKEY_ADDRESS, new byte[] { 65 })
        .SetBase58Bytes(Base58Type.SCRIPT_ADDRESS, new byte[] { 12 })
        .SetBase58Bytes(Base58Type.SECRET_KEY, new byte[] { 239 })
        .SetBase58Bytes(Base58Type.EXT_PUBLIC_KEY, new byte[] { 0x3a, 0x80, 0x61, 0xa0 })
        .SetBase58Bytes(Base58Type.EXT_SECRET_KEY, new byte[] { 0x3a, 0x80, 0x58, 0x37 })
        .SetBech32(Bech32Type.WITNESS_PUBKEY_ADDRESS, Encoders.Bech32("thrld"))
        .SetBech32(Bech32Type.WITNESS_SCRIPT_ADDRESS, Encoders.Bech32("thrld"))
        .SetMagic(0x51b3aba0)
        .SetPort(43246)
        .SetRPCPort(43247)
        .SetName("haroldcoin-test")
        .AddAlias("haroldcoin-testnet")
        .AddSeeds(new NetworkAddress[0])
        .SetGenesis("00000f2fddbb7212e6f36f398461e8ad49dba752608c9c7322cb97e9a893b485");

        return builder;
    }

    protected override NetworkBuilder CreateRegtest()
    {
        var builder = new NetworkBuilder();
        builder.SetConsensus(new Consensus()
        {
            SubsidyHalvingInterval = 0,
            MajorityEnforceBlockUpgrade = 750,
            MajorityRejectBlockOutdated = 950,
            MajorityWindow = 1000,
            PowLimit = new Target(new uint256("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")),
            PowTargetTimespan = TimeSpan.FromSeconds(1 * 30),
            PowTargetSpacing = TimeSpan.FromSeconds(5 * 60),
            PowAllowMinDifficultyBlocks = true,
            PowNoRetargeting = true,
            RuleChangeActivationThreshold = 1,
            MinerConfirmationWindow = 2,
            CoinbaseMaturity = 15,
            ConsensusFactory = HaroldcoinConsensusFactory.Instance
        })
        .SetBase58Bytes(Base58Type.PUBKEY_ADDRESS, new byte[] { 65 })
        .SetBase58Bytes(Base58Type.SCRIPT_ADDRESS, new byte[] { 12 })
        .SetBase58Bytes(Base58Type.SECRET_KEY, new byte[] { 239 })
        .SetBase58Bytes(Base58Type.EXT_PUBLIC_KEY, new byte[] { 0x3a, 0x80, 0x61, 0xa0 })
        .SetBase58Bytes(Base58Type.EXT_SECRET_KEY, new byte[] { 0x3a, 0x80, 0x58, 0x37 })
        .SetBech32(Bech32Type.WITNESS_PUBKEY_ADDRESS, Encoders.Bech32("rhrld"))
        .SetBech32(Bech32Type.WITNESS_SCRIPT_ADDRESS, Encoders.Bech32("rhrld"))
        .SetMagic(0xfabfb5da)
        .SetPort(43248)
        .SetRPCPort(43249)
        .SetName("haroldcoin-regtest")
        .AddAlias("haroldcoin-regtest")
        .AddSeeds(new NetworkAddress[0])
        .SetGenesis("00000f2fddbb7212e6f36f398461e8ad49dba752608c9c7322cb97e9a893b485");

        return builder;
    }
}