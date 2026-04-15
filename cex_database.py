"""
Base de dados de endereços e programas conhecidos de exchanges (CEX) na Solana.
Fontes: Solscan labels, SolanaFM entity tags, registros públicos.
"""

# Endereços de depósito/hot wallets de CEXs conhecidas na Solana
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
    # FTX (histórico - falido em 2022)
    "2oogpTYm1sp6LPZAWD3bp2wsFpnV2kXL1s52yyFhW5vT": "FTX (histórico)",
    "9BVcYqEQxyccuwznvxXqDkSJFavvTyheiTYk231T1A8S": "FTX (histórico)",
}

# Nomes de CEXs para detecção textual em labels e descrições
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
}

# Programas de bridge conhecidos (cruzamento de chain)
BRIDGE_PROGRAMS: dict[str, str] = {
    "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth": "Wormhole Bridge",
    "Bridge1p5gheXUvJ6jGWGeCsgPKgnE3YgdGKRVCMY9o": "Portal Bridge",
    "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof": "Allbridge",
    "deChargefPBJQBpkk5rkGJxb9G8MERk6TBNMR15Hqgz": "deBridge",
    "Ax9ujW5B9oqcv59N8m6f1BpTBq2rGeGaBcpKjwm6cjNY": "Wormhole NTT",
}

# Programas DeFi comuns (swap laundering)
DEFI_PROGRAMS: dict[str, str] = {
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter Aggregator v6",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB": "Jupiter Aggregator v4",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca Whirlpools",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM v4",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin": "Serum DEX v3",
    "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo": "Solend",
    "MFv2hWf31Z9kbCa1snEPdcgp7RtWHygni3F5ZRuWJep": "MarginFi",
    "KLend2g3cP87fffoy8q1mQqGKjrL1yfghkSKtYpSsqW": "Kamino Finance",
}


def is_cex_address(address: str) -> tuple[bool, str]:
    """Verifica se um endereço é uma CEX conhecida. Retorna (is_cex, exchange_name)."""
    if address in CEX_ADDRESSES:
        return True, CEX_ADDRESSES[address]
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
    if address in DEFI_PROGRAMS:
        return {"type": "DeFi Protocol", "name": DEFI_PROGRAMS[address], "risk": "MEDIUM RISK"}
    return {"type": "Unknown Wallet", "name": "Desconhecido", "risk": "UNKNOWN"}
