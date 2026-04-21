"""
Base de dados de endereços e programas conhecidos na Solana.
Cobre: CEX, DEX, AMM Pools, Bridges, Lending, Staking, NFT Marketplaces.
Fontes: Solscan labels, SolanaFM entity tags, Jupiter program registry, registros públicos.
"""

# ─────────────────────────────────────────────────────────────────────────────
# CEX — Endereços de depósito / hot wallets
# ─────────────────────────────────────────────────────────────────────────────

CEX_ADDRESSES: dict[str, str] = {
    # Binance
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": "Binance",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "Binance",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Binance",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance",
    "YDYaCtAEUJJHNGaZEGnMHDqPJf72bGGHuFdHyj8Zqbr": "Binance",
    "Cbdegp5cBvqFgVtR3GKZKJ6D8xNbQ4oM2u8LXQHK2zV": "Binance",
    "HVh6wHNBAsnt42Jk8xmBaBMqgLnN5JxrKBMoJhHfJFJQ": "Binance",
    # Coinbase
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase",
    "oQPnhXAbLbMuKHESaGrbXT17CyvWCpLyERSJA9HCYd7": "Coinbase",
    "GvjNejXvjkNxnJf8aQXCfH4pVZGKLZRFSiY4M3J5yXWb": "Coinbase",
    # Kraken
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "Kraken",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "Kraken",
    # OKX
    "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm": "OKX",
    "okv4eCaQaFHGERPaKmGEJGBnvYCTMahVRuCMUyFBhPo": "OKX",
    "B2Cj7ZETLQ7VFCjN2MdBJJbKWdFbXWL6nMaCn1oxFUa8": "OKX",
    # Bybit
    "A77HErqtfN1hLLpvZ9pGtu7oECGGDZAFMDHAN9AcnxA5": "Bybit",
    "CEzN7mqP9xoxn2HdyW6fjEJ73t7qaX9Rp2zyS6hb3iEy": "Bybit",
    # KuCoin
    "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6": "KuCoin",
    "GUfCR9mK6azb9vcpsxgXyj7XRPAKJd4KMHTTVvtncGgp": "KuCoin",
    # Bitfinex
    "8UViNr47S29ATqoiNX5kAQ1czSktBdXhK4tEDf9RNjfT": "Bitfinex",
    # Gate.io
    "HKuJrP5tYQLbEUdjKwjgnHs2957QKjR2NTmNq7CVbQ9S": "Gate.io",
    # MEXC
    "3fTR8GGL2mniGyHtd3Qy2KDVhZ9LHbW59rCc7A3RtBWk": "MEXC",
    # Huobi / HTX
    "AVLhahDcDQ4m4vHM4ug72oh7FE3X8CqtCHHcBFECVvGM": "Huobi/HTX",
    # FTX (histórico)
    "2oogpTYm1sp6LPZAWD3bp2wsFpnV2kXL1s52yyFhW5vT": "FTX (histórico)",
    "9BVcYqEQxyccuwznvxXqDkSJFavvTyheiTYk231T1A8S": "FTX (histórico)",
}

# ─────────────────────────────────────────────────────────────────────────────
# CEX — Padrões de nome para detecção textual
# ─────────────────────────────────────────────────────────────────────────────

CEX_NAME_PATTERNS: dict[str, str] = {
    "binance": "Binance",
    "coinbase": "Coinbase",
    "kraken": "Kraken",
    "okx": "OKX",
    "bybit": "Bybit",
    "kucoin": "KuCoin",
    "ftx": "FTX",
    "gate.io": "Gate.io",
    "gateio": "Gate.io",
    "huobi": "Huobi/HTX",
    "htx": "Huobi/HTX",
    "mexc": "MEXC",
    "bitfinex": "Bitfinex",
    "crypto.com": "Crypto.com",
    "bitget": "Bitget",
    "bingx": "BingX",
    "phemex": "Phemex",
    "upbit": "Upbit",
    "bithumb": "Bithumb",
    "bitstamp": "Bitstamp",
    "gemini": "Gemini",
}

# ─────────────────────────────────────────────────────────────────────────────
# BRIDGE PROGRAMS — Cruzamento cross-chain
# ─────────────────────────────────────────────────────────────────────────────

BRIDGE_PROGRAMS: dict[str, str] = {
    # Wormhole
    "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth": "Wormhole Bridge",
    "Bridge1p5gheXUvJ6jGWGeCsgPKgnE3YgdGKRVCMY9o": "Portal Bridge (Wormhole)",
    "Ax9ujW5B9oqcv59N8m6f1BpTBq2rGeGaBcpKjwm6cjNY": "Wormhole NTT",
    "HDwcJBJXjL9FpJ7UBsYBtaDjsBUhuLCUYoz3zr8SWWaQ": "Wormhole Token Bridge",
    # Allbridge
    "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof": "Allbridge",
    "A9mUU4qviSctJVPJdBJWkb28deg915LYJKrzQ19ji3FM": "Allbridge Core",
    # deBridge
    "deChargefPBJQBpkk5rkGJxb9G8MERk6TBNMR15Hqgz": "deBridge",
    "src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22Sf6Ax": "deBridge Source",
    # Mayan Finance
    "FC4eXxkyrMPTjiYUpp4EAnkmwMbQyZ9KSW9d5AKKKcvs": "Mayan Finance Bridge",
    "mayanMigrator2ndQGjMFQiECMBJKnP8tDt43kKmqHM8": "Mayan Migrator",
    # Synapse
    "synapseprotoco1PrjEd9DjiNpyEhQpNKMoSXcD5gZoS": "Synapse Bridge",
    # Celer cBridge
    "cBridgePausedaddress111111111111111111111111": "Celer cBridge",
    # Stargate / LayerZero
    "StAKeNyMxi6xBohWcBGt54fMDVBHh3enJLJMDGxs7cr": "Stargate Finance",
}

# ─────────────────────────────────────────────────────────────────────────────
# DEX PROGRAMS — Agregadores e roteadores de swap
# ─────────────────────────────────────────────────────────────────────────────

DEX_AGGREGATOR_PROGRAMS: dict[str, str] = {
    # Jupiter (maior agregador Solana)
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter Aggregator v6",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB": "Jupiter Aggregator v4",
    "JUP3c2Uh3WA4Ng34tw6kPd2G4YotpViXWQkVECwALGW": "Jupiter Aggregator v3",
    "JUP2jxvXaqu7NQY1GmNF4m1vodwdXNGBXKN8ugX5Lro": "Jupiter Aggregator v2",
    "jupoNjAxXgZ4rjzxzPMP4XXi1yoNo3jqZcqtguM9GGJ": "Jupiter DCA",
    "j1o2qRpjcyUwEvwtcfhEQefh773ZgjxcVRry7LDqg5X": "Jupiter Limit Orders",
    # 1inch (Solana)
    "1inch1FkBskeeyJOJtHyxkdmKiTrkY5Bksp5TKFKLSN": "1inch Fusion Solana",
    # Odos
    "7WduLbRfYhTJkt3BFZBbp2iQ3EzJJFDdcXXrBcx8qdZd": "Odos Router",
}

# ─────────────────────────────────────────────────────────────────────────────
# DEX PROGRAMS — AMMs e pools de liquidez
# ─────────────────────────────────────────────────────────────────────────────

DEX_AMM_PROGRAMS: dict[str, str] = {
    # Orca
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca Whirlpools",
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": "Orca v2",
    "DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1": "Orca Aquafarm",
    # Raydium
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM v4",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h": "Raydium AMM v3",
    "RVKd61ztZW9GUwhRbbLoYVRE5Xf1B2tVscKqwZqXgEr": "Raydium Liquidity Pool v2",
    "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv": "Raydium Stable Swap",
    "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS": "Raydium Route Swap",
    # Meteora
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EkVnGE9n": "Meteora Dynamic Pools",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K": "Meteora Stable Pools",
    "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSohnc4XS5K": "Meteora Flux AMM",
    # Lifinity
    "EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S": "Lifinity AMM v1",
    "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c": "Lifinity AMM v2",
    # Saber
    "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ": "Saber Stable Swap",
    "SaberwSwapAMMjRmaTwtWDeotnU65ERC6ZXZ61KHbdj1": "Saber Router",
    # Aldrin
    "AMM55ShdkoioZB5LzcqgGYBCpiHnSnbQMqrha1zfaHgR": "Aldrin AMM v2",
    # Crema Finance
    "CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR": "Crema Finance CLMM",
    # Invariant
    "HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJuyz": "Invariant Protocol",
    # Serum / OpenBook
    "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin": "Serum DEX v3",
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX": "Serum v3 Bonfida",
    "opnb2LAfJYbRMAHHvqjCwQxanZn7n7QM7qHbeFkurtm": "OpenBook v2",
    "EoTcMgcDRTJVZFMudgEC2RosWKdGHPlCGRV13f6GxH3": "OpenBook AMM",
    # Dradex
    "dr1xyrzJDyDVBcpHBXEFUUkFHMVFbz3gDhNVDEW3R7g": "Dradex",
    # Sanctum
    "5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx": "Sanctum Router",
    "SAnctu1on11111111111111111111111111111111111": "Sanctum Liquid Staking",
}

# ─────────────────────────────────────────────────────────────────────────────
# DEFI PROGRAMS — Lending, borrowing, yield
# ─────────────────────────────────────────────────────────────────────────────

DEFI_PROGRAMS: dict[str, str] = {
    # Jupiter (mantido por compatibilidade)
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter Aggregator v6",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB": "Jupiter Aggregator v4",
    # Orca
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca Whirlpools",
    # Raydium
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM v4",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    # Lending
    "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo": "Solend",
    "MFv2hWf31Z9kbCa1snEPdcgp7RtWHygni3F5ZRuWJep": "MarginFi",
    "KLend2g3cP87fffoy8q1mQqGKjrL1yfghkSKtYpSsqW": "Kamino Finance",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bsn": "Kamino Lending",
    "Port7uDYB3wkM4GE6HDAktfRovzXiLewGeXU3jB5jAA": "Port Finance",
    "VoLT1mJz1sbnxwq5Fv2SXjdVDgPXrb9tJyC8WpMDkSp": "Volt Protocol",
    "FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn": "FlashTrade",
    # Staking líquido
    "MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD": "Marinade Finance",
    "CrX7kMhLC3cSsXJdT7JDgqrRVWGnUpX3gfEfxxU2NVLi": "Marinade Staking",
    "Jito111111111111111111111111111111111111111112": "Jito Staking",
    "jitodontlosethisforyourownsafety111111111111": "Jito MEV Tips",
    "SPoo1Ku8WFXoNDMHPsrGSTx1YTda8g6oo2GqHaVP1wR": "Socean Staking",
    "stakeSSzfxn391k3LvdKbZP5WVwWd6AsY1DNiXHjQfK": "Lido Staked SOL",
    "7ge2xKsZXmqPxa3YmXxXmzCJl6QpnGKTosqq1LfRKqiR": "BlazeStake",
    # Perps / Options
    "PERPHjGBqRHArX4DySjwM6rrXsJ1JQF36KqChTQoMbj": "Perpetuals Protocol",
    "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH": "Drift Protocol",
    "9jtAfMSMTMXvJEXhFjBEVWuPNdN4eCCPBFSC4N8nycPT": "Zeta Markets",
    "opDa1NaNb4EM3e7TwXcmEQSdNiWMcAy9XRVwb8EYuJ1": "Cypher Protocol",
    # NFT Marketplaces (transferências podem passar por aqui)
    "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K": "Magic Eden v2",
    "mmm3XBJg5gk8XJa8TSbx7Rv99i5XRtyDSZpRTLEuTFD": "Magic Eden v3",
    "TSWAPaqyCSx2KABk68Shruf4rp7CxcAi9von1D84AmY": "Tensor Swap",
    "HYPERfwdTjyJ2SCaKHmpF2MtrXqWxrsotYDsTrshHWq8": "Hyperspace NFT",
    "hausS13jySowiJMjSfTQ87vmUsaAAgmC8oe25hVGMCR": "Hadeswap",
}

# ─────────────────────────────────────────────────────────────────────────────
# STABLECOIN MINTS — Para identificação de tokens em transfers
# ─────────────────────────────────────────────────────────────────────────────

STABLECOIN_MINTS: dict[str, str] = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
    "USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX": "USDH",
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj": "stSOL",
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": "mSOL",
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1": "bSOL",
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn": "jitoSOL",
}

# ─────────────────────────────────────────────────────────────────────────────
# PROGRAM CATEGORIES — Mapa completo para lookup rápido
# ─────────────────────────────────────────────────────────────────────────────

# Todos os programas DEX (agregadores + AMMs) num único dict
ALL_DEX_PROGRAMS: dict[str, str] = {
    **DEX_AGGREGATOR_PROGRAMS,
    **DEX_AMM_PROGRAMS,
}

# Todos os programas conhecidos (DEX + DeFi + Bridge) num único dict
ALL_KNOWN_PROGRAMS: dict[str, str] = {
    **DEX_AGGREGATOR_PROGRAMS,
    **DEX_AMM_PROGRAMS,
    **DEFI_PROGRAMS,
    **BRIDGE_PROGRAMS,
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def is_cex_address(address: str) -> tuple[bool, str]:
    """Verifica se um endereço é uma CEX conhecida."""
    if address in CEX_ADDRESSES:
        return True, CEX_ADDRESSES[address]
    return False, ""


def is_dex_program(address: str) -> tuple[bool, str]:
    """Verifica se um endereço é um programa DEX/AMM conhecido."""
    if address in ALL_DEX_PROGRAMS:
        return True, ALL_DEX_PROGRAMS[address]
    return False, ""


def is_bridge_program(address: str) -> tuple[bool, str]:
    """Verifica se um endereço é um programa de bridge conhecido."""
    if address in BRIDGE_PROGRAMS:
        return True, BRIDGE_PROGRAMS[address]
    return False, ""


def is_defi_program(address: str) -> tuple[bool, str]:
    """Verifica se um endereço é um programa DeFi conhecido (lending, staking, etc)."""
    if address in DEFI_PROGRAMS:
        return True, DEFI_PROGRAMS[address]
    return False, ""


def detect_cex_from_label(label: str) -> tuple[bool, str]:
    """Detecta CEX a partir de um label textual."""
    label_lower = label.lower()
    for pattern, name in CEX_NAME_PATTERNS.items():
        if pattern in label_lower:
            return True, name
    return False, ""


def get_entity_info(address: str) -> dict:
    """Retorna informações completas sobre uma entidade identificada."""
    if address in CEX_ADDRESSES:
        return {"type": "CEX", "name": CEX_ADDRESSES[address], "risk": "MEDIUM RISK"}
    if address in BRIDGE_PROGRAMS:
        return {"type": "Bridge", "name": BRIDGE_PROGRAMS[address], "risk": "HIGH RISK"}
    if address in DEX_AGGREGATOR_PROGRAMS:
        return {"type": "DEX Aggregator", "name": DEX_AGGREGATOR_PROGRAMS[address], "risk": "LOW RISK"}
    if address in DEX_AMM_PROGRAMS:
        return {"type": "DEX AMM/Pool", "name": DEX_AMM_PROGRAMS[address], "risk": "LOW RISK"}
    if address in DEFI_PROGRAMS:
        return {"type": "DeFi Protocol", "name": DEFI_PROGRAMS[address], "risk": "LOW RISK"}
    return {"type": "Unknown Wallet", "name": "Desconhecido", "risk": "UNKNOWN"}


def classify_address(address: str) -> dict:
    """
    Classificação completa de um endereço.
    Retorna: type, name, risk, is_cex, is_dex, is_bridge, is_defi, is_known
    """
    info = get_entity_info(address)
    return {
        **info,
        "is_cex":    address in CEX_ADDRESSES,
        "is_dex":    address in ALL_DEX_PROGRAMS,
        "is_bridge": address in BRIDGE_PROGRAMS,
        "is_defi":   address in DEFI_PROGRAMS,
        "is_known":  address in ALL_KNOWN_PROGRAMS or address in CEX_ADDRESSES,
    }
