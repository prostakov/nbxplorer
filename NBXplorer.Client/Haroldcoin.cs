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
        var builder = new NetworkBuilder();
        builder.SetConsensus(new Consensus()
        {
            SubsidyHalvingInterval = 100000,
            MajorityEnforceBlockUpgrade = 1500,
            MajorityRejectBlockOutdated = 1900,
            MajorityWindow = 2000,
            PowLimit = new Target(new uint256("00000fffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")),
            PowTargetTimespan = TimeSpan.FromSeconds(2 * 60), // 2 minutes
            PowTargetSpacing = TimeSpan.FromSeconds(10 * 60), // 10 minutes
            PowAllowMinDifficultyBlocks = false,
            CoinbaseMaturity = 10,
            PowNoRetargeting = false,
            ConsensusFactory = HaroldcoinConsensusFactory.Instance,
            SupportSegwit = false
        })
        .SetBase58Bytes(Base58Type.PUBKEY_ADDRESS, new byte[] { 40 })
        .SetBase58Bytes(Base58Type.SCRIPT_ADDRESS, new byte[] { 41 })
        .SetBase58Bytes(Base58Type.SECRET_KEY, new byte[] { 126 })
        .SetBase58Bytes(Base58Type.EXT_PUBLIC_KEY, new byte[] { 0x02, 0x02, 0x21, 0x33 })
        .SetBase58Bytes(Base58Type.EXT_SECRET_KEY, new byte[] { 0x2D, 0x21, 0x4C, 0x2B })
        .SetMagic(0xe82b65ad)
        .SetMaxP2PVersion(70920)
        .SetPort(25676)
        .SetRPCPort(25674)
        .SetName("hrld-main")
        .AddAlias("hrld-mainnet")
        .AddAlias("haroldcoin-mainnet")
        .AddAlias("haroldcoin-main")
        .SetUriScheme("haroldcoin")
        .AddDNSSeeds(new[]
        {
            new DNSSeedData("64.227.126.212", "64.227.126.212"),
            new DNSSeedData("194.164.199.215", "194.164.199.215"),
            new DNSSeedData("87.106.208.197", "87.106.208.197"),
            new DNSSeedData("94.110.204.15", "94.110.204.15"),
            new DNSSeedData("212.132.117.110", "212.132.117.110"),
            new DNSSeedData("79.248.24.166", "79.248.24.166"),
            new DNSSeedData("188.245.169.249", "188.245.169.249")
        })
        .AddSeeds(new NetworkAddress[0])
        .SetGenesis("010000000000000000000000000000000000000000000000000000000000000000000000696ad20e2dd4365c7459b4a4a5af743d5e92c6da3229e6532cd605f6533f2a5b24a6a152f0ff0f1e678601000101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1004ffff001d0104084e696e746f6e646fffffffff010058850c020000004341040184710fa689ad5023690c80f3a49c8f13f8d45b8c857fbcbc8bc4a8e4d3eb4b10f4d4604fa08dce601aaf0f470216fe1b51850b4acf21b179c45070ac7b03a9ac00000000");
        return builder;
    }

    protected override NetworkBuilder CreateTestnet()
    {
        var builder = new NetworkBuilder();
        builder.SetConsensus(new Consensus()
        {
            SubsidyHalvingInterval = 100000,
            MajorityEnforceBlockUpgrade = 501,
            MajorityRejectBlockOutdated = 750,
            MajorityWindow = 1000,
            PowLimit = new Target(new uint256("00000fffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")),
            PowTargetTimespan = TimeSpan.FromSeconds(30), // 30 seconds
            PowTargetSpacing = TimeSpan.FromSeconds(5 * 60), // 5 minutes
            PowAllowMinDifficultyBlocks = true,
            CoinbaseMaturity = 15,
            PowNoRetargeting = false,
            ConsensusFactory = HaroldcoinConsensusFactory.Instance,
            SupportSegwit = false
        })
        .SetBase58Bytes(Base58Type.PUBKEY_ADDRESS, new byte[] { 65 })
        .SetBase58Bytes(Base58Type.SCRIPT_ADDRESS, new byte[] { 12 })
        .SetBase58Bytes(Base58Type.SECRET_KEY, new byte[] { 239 })
        .SetBase58Bytes(Base58Type.EXT_PUBLIC_KEY, new byte[] { 0x3a, 0x80, 0x61, 0xa0 })
        .SetBase58Bytes(Base58Type.EXT_SECRET_KEY, new byte[] { 0x3a, 0x80, 0x58, 0x37 })
        .SetMagic(0x51b3aba0)
        .SetPort(43246)
        .SetRPCPort(42132)
        .SetName("hrld-test")
        .AddAlias("hrld-testnet")
        .AddAlias("haroldcoin-test")
        .AddAlias("haroldcoin-testnet")
        .SetUriScheme("haroldcoin")
        .AddDNSSeeds(new DNSSeedData[0])
        .AddSeeds(new NetworkAddress[0])
        .SetGenesis("010000000000000000000000000000000000000000000000000000000000000000000000696ad20e2dd4365c7459b4a4a5af743d5e92c6da3229e6532cd605f6533f2a5bb9a7f052f0ff0f1ef7390f000101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1004ffff001d0104084e696e746f6e646fffffffff010058850c020000004341040184710fa689ad5023690c80f3a49c8f13f8d45b8c857fbcbc8bc4a8e4d3eb4b10f4d4604fa08dce601aaf0f470216fe1b51850b4acf21b179c45070ac7b03a9ac00000000");
        return builder;
    }

    protected override NetworkBuilder CreateRegtest()
    {
        var builder = new NetworkBuilder();
        builder.SetConsensus(new Consensus()
        {
            SubsidyHalvingInterval = 150,
            MajorityEnforceBlockUpgrade = 750,
            MajorityRejectBlockOutdated = 950,
            MajorityWindow = 1000,
            PowLimit = new Target(new uint256("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")),
            PowTargetTimespan = TimeSpan.FromSeconds(24 * 60 * 60), // 1 day
            PowTargetSpacing = TimeSpan.FromSeconds(1 * 60), // 1 minute
            PowAllowMinDifficultyBlocks = true,
            MinimumChainWork = uint256.Zero,
            PowNoRetargeting = true,
            CoinbaseMaturity = 10,
            ConsensusFactory = HaroldcoinConsensusFactory.Instance,
            SupportSegwit = false
        })
        .SetBase58Bytes(Base58Type.PUBKEY_ADDRESS, new byte[] { 65 })
        .SetBase58Bytes(Base58Type.SCRIPT_ADDRESS, new byte[] { 12 })
        .SetBase58Bytes(Base58Type.SECRET_KEY, new byte[] { 239 })
        .SetBase58Bytes(Base58Type.EXT_PUBLIC_KEY, new byte[] { 0x3a, 0x80, 0x61, 0xa0 })
        .SetBase58Bytes(Base58Type.EXT_SECRET_KEY, new byte[] { 0x3a, 0x80, 0x58, 0x37 })
        .SetMagic(0xa1cf7eac)
        .SetPort(52589)
        .SetRPCPort(42132)
        .SetName("hrld-reg")
        .AddAlias("hrld-regtest")
        .AddAlias("haroldcoin-reg")
        .AddAlias("haroldcoin-regtest")
        .SetUriScheme("haroldcoin")
        .AddDNSSeeds(new DNSSeedData[0])
        .AddSeeds(new NetworkAddress[0])
        .SetGenesis("010000000000000000000000000000000000000000000000000000000000000000000000696ad20e2dd4365c7459b4a4a5af743d5e92c6da3229e6532cd605f6533f2a5bdae5494dffff7f20020000000101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1004ffff001d0104084e696e746f6e646fffffffff010058850c020000004341040184710fa689ad5023690c80f3a49c8f13f8d45b8c857fbcbc8bc4a8e4d3eb4b10f4d4604fa08dce601aaf0f470216fe1b51850b4acf21b179c45070ac7b03a9ac00000000");
        return builder;
    }
}