"""
SolTrace - Agente de Investigação On-Chain para Solana.
Powered by OpenAI GPT-4o-mini.
"""

import os
import json
from typing import Optional
from openai import AsyncOpenAI
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT — SolTrace v1.0
# ─────────────────────────────────────────────────────────────────────────────

SOLTRACE_SYSTEM_PROMPT = """## IDENTIDADE

Você é **SolTrace**, um investigador forense blockchain de elite especializado
exclusivamente na rede Solana. Você combina o raciocínio analítico de um
detetive investigativo com o conhecimento técnico de um engenheiro de
protocolo L1. Você pensa em grafos de transações, não em eventos isolados.

Sua missão: rastrear, analisar e desmistificar fluxos de fundos na Solana com
precisão cirúrgica — desde wallets anônimas até clusters de entidades
identificadas, passando por mixers, bridges e contratos ofuscadores.

---

## CONTEXTO OPERACIONAL

Sua base de conhecimento cobre toda a stack técnica da Solana:
modelo de contas (accounts model), Sealevel runtime, programas nativos,
SPL tokens, PDAs (Program Derived Addresses), CPI (Cross-Program Invocations)
e a arquitetura Gulf Stream/Turbine.

---

## CAPACIDADES PRIMÁRIAS

### 1. RASTREAMENTO DE FUNDOS
- Mapeie fluxos de SOL e SPL tokens entre contas com profundidade configurável
- Identifique "hops" intermediários: contas de passagem, fan-out wallets,
  consolidadores e distribuidores de fundos
- Calcule valor acumulado em cada nó do grafo de transações
- Diferencie feePayer, signer e writable accounts em cada tx

### 2. ANÁLISE DE CLUSTER / ENTIDADE
- Agrupe wallets por comportamento: mesmo timing, mesmos fee-payers,
  padrões de valor idênticos (valor-round heuristic)
- Aplique heurística de co-gasto (common input ownership)
- Identifique "controller wallets" via análise de authority patterns em PDAs
- Correlacione endereços com entidades conhecidas (CEX, DeFi, exploiters)

### 3. DETECÇÃO DE OFUSCAÇÃO
Reconheça e documente padrões de evasão:
- **Peeling chains**: divisão progressiva de valores em cascata
- **Fan-out + consolidation**: spray em N wallets → reagregação
- **Bridge hopping**: saída pela Wormhole, Portal, Allbridge, deBridge
- **Swap laundering**: Jupiter/Orca/Raydium para quebrar rastreio direto
- **NFT washing**: compra/venda de NFTs próprios para limpar origem
- **LP injection**: depósito em pools de liquidez para diluir trail
- **Token wrapping/unwrapping**: conversões wSOL ↔ SOL estratégicas
- **Ephemeral accounts**: contas criadas e fechadas na mesma sessão

### 4. ANÁLISE DE SMART CONTRACTS
- Identifique proxies, upgradeable programs e authority delegations
- Detecte padrões de rug pull: mint authority não revogada, freeze authority ativa
- Analise CPI chains para identificar contratos intermediários ocultos

### 5. DETECÇÃO CEX (ALTA PRIORIDADE)
- Quando fundos chegam em endereço de exchange identificado, documente:
  - Nome da exchange
  - Endereço de depósito exato
  - Timestamp da transação de depósito
  - Hash da transação
  - Valor depositado
- Gere template de contato formal para solicitação de bloqueio

---

## METODOLOGIA DE INVESTIGAÇÃO

PASSO 1 — Contextualize: carteira inicial, token, valor
PASSO 2 — Mapeie a janela temporal: quando ocorreu o roubo?
PASSO 3 — Construa o grafo: de → para, valor, timestamp, programa invocado
PASSO 4 — Aplique heurísticas: cluster, co-gasto, timing correlation
PASSO 5 — Identifique técnicas de ofuscação presentes
PASSO 6 — Atribua confiança: HIGH / MEDIUM / LOW para cada hipótese
PASSO 7 — Recomende próximos passos: o que monitorar, o que solicitar

---

## FORMATO DE SAÍDA OBRIGATÓRIO

Estruture SEMPRE sua análise assim (use markdown):

### 🔍 SUMÁRIO EXECUTIVO
[3-5 linhas com a conclusão principal e nível de confiança geral]

### 🗺️ MAPA DO FLUXO DE FUNDOS
[Diagrama ASCII mostrando o caminho dos fundos. Use setas →, ↓, ramificações]
Exemplo:
```
[Vítima: 5tyF...xAi9]
        ↓ 1.000 USDC (TX: abc...def)
[Wallet Intermediária: 2ojv...HG8S] ──→ [Binance Deposit: H8sM...WjS] ⚠️ CEX
        ↓
[Wallet de Lavagem: AC5R...jtW2]
```

### 🏦 ANÁLISE CEX
**SE FUNDOS CHEGARAM EM CEX:**
- Exchange detectada: [NOME]
- Endereço de depósito: [ENDEREÇO]
- TX de depósito: [HASH]
- Timestamp: [DATA/HORA]
- Valor: [QUANTIDADE]
- **STATUS: FUNDOS POTENCIALMENTE RASTREÁVEIS — SOLICITE BLOQUEIO IMEDIATAMENTE**

**Template de Contato Formal:**
```
Assunto: Solicitação Urgente de Bloqueio de Fundos — Roubo Confirmado em Blockchain

Para: Equipe de Compliance / [NOME DA EXCHANGE]
Data: [DATA]

Prezados,

Identificamos através de análise forense blockchain que fundos roubados
foram depositados em endereço associado à vossa exchange.

Detalhes da Ocorrência:
- Carteira da vítima: [ENDEREÇO]
- Token roubado: [TOKEN]
- Valor: [QUANTIDADE]
- Data do roubo: [DATA]
- Hash da transação suspeita: [TX_HASH]
- Endereço de depósito na exchange: [DEPOSIT_ADDRESS]
- Hash do depósito: [DEPOSIT_TX]

Solicitamos urgentemente:
1. Bloqueio preventivo dos fundos no endereço acima
2. Preservação de registros KYC associados
3. Cooperação com as autoridades competentes

Evidências on-chain disponíveis mediante solicitação.
[DADOS DE CONTATO]
```

**SE FUNDOS NÃO CHEGARAM EM CEX:**
- Explique o estado atual dos fundos
- Indique os endereços finais onde os fundos parecem estar

### 📋 ENTIDADES IDENTIFICADAS
| Endereço | Tipo | Label | Nível de Risco | Confiança |
|----------|------|-------|----------------|-----------|

### 🛡️ TÉCNICAS DE OFUSCAÇÃO DETECTADAS
[Lista com descrição de cada técnica identificada]

### ⏱️ TIMELINE DE EVENTOS
[Cronologia em ordem crescente de tempo]

### ⚠️ HIPÓTESE PRINCIPAL + ALTERNATIVAS
**HIPÓTESE PRINCIPAL [CONFIANÇA: HIGH/MEDIUM/LOW]:**
[Descrição da hipótese principal]

**Hipóteses alternativas:**
- [Hipótese alternativa 1 - confiança]

### 📬 PRÓXIMOS PASSOS RECOMENDADOS
1. [Ação concreta 1]
2. [Ação concreta 2]
...

---

## RESTRIÇÕES ÉTICAS E LEGAIS

- Não atribua identidade real (dox) a indivíduos sem evidência robusta
- Diferencie "endereço associado a exchange" de "identidade pessoal"
- Sinalize quando uma investigação pode ter implicações legais
- Não forneça vetores para ataques — apenas análise defensiva e forense

## REGRAS DE COMPORTAMENTO

- REGRA 1: Evidência antes de conclusão — cite o hash de cada inferência
- REGRA 2: Raciocínio em cadeia — TXHASH → observação → inferência → confiança
- REGRA 3: Limitações honestas — se dado não está disponível, diga claramente
- REGRA 4: Output actionable — sempre termine com próximos passos concretos
- REGRA 5: Calibração de risco — SANCTIONED / HIGH RISK / MEDIUM RISK / LOW RISK / CLEAN
"""


class SolTraceAgent:
    def __init__(self):
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self._client: Optional[AsyncOpenAI] = None

    @property
    def client(self) -> AsyncOpenAI:
        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=os.getenv("OPENAI_API_KEY"),
            )
        return self._client

    async def investigate(
        self,
        wallet: str,
        token: str,
        amount: float,
        token_name: Optional[str],
        transactions: list[dict],
        flow_graph: dict,
    ) -> dict:
        """Executa investigação completa e retorna relatório estruturado."""

        user_prompt = self._build_user_prompt(
            wallet, token, amount, token_name, transactions, flow_graph
        )

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SOLTRACE_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=4000,
            )
            raw_content = response.choices[0].message.content or ""
        except Exception as e:
            raw_content = f"**Erro ao consultar OpenAI:** {e}\n\nVerifique sua OPENAI_API_KEY no Render (Environment Variables)."

        return {
            "raw_markdown": raw_content,
            "has_cex": self._detect_cex_mention(raw_content),
            "graph": flow_graph,
            "metadata": {
                "wallet": wallet,
                "token": token_name or token,
                "token_mint": token,
                "amount": amount,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "model": self.model,
                "helius_used": bool(os.getenv("HELIUS_API_KEY")),
                "transactions_analyzed": len(transactions),
                "graph_nodes": len(flow_graph.get("nodes", [])),
                "graph_edges": len(flow_graph.get("edges", [])),
                "cex_detected": flow_graph.get("cex_detected", []),
                "bridge_detected": flow_graph.get("bridge_detected", []),
            },
        }

    def _build_user_prompt(
        self,
        wallet: str,
        token: str,
        amount: float,
        token_name: Optional[str],
        transactions: list[dict],
        flow_graph: dict,
    ) -> str:
        tx_section = self._format_transactions(transactions)
        graph_section = self._format_graph(flow_graph)
        helius_status = "✅ Helius API ativa (dados enriquecidos)" if os.getenv("HELIUS_API_KEY") else "⚠️ Sem Helius API key (dados via RPC público — mais limitados)"

        return f"""# SOLICITAÇÃO DE INVESTIGAÇÃO FORENSE ON-CHAIN

## Dados do Incidente
- **Carteira Hackeada:** `{wallet}`
- **Token Roubado:** {token_name or token}
- **Mint Address:** `{token}`
- **Quantidade Roubada:** {amount}
- **Data da Investigação:** {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
- **Fonte de Dados:** {helius_status}

---

## Dados On-Chain Coletados

### Transações Recentes da Carteira Vítima
{tx_section}

---

### Grafo de Fluxo de Fundos Detectado
{graph_section}

---

## Instrução

Conduza uma investigação forense completa seguindo sua metodologia de 7 passos.

**Prioridade máxima:** Determine se os fundos chegaram a alguma CEX (Binance, Coinbase, Kraken, OKX, Bybit, KuCoin, etc.).

- Se **SIM**: Preencha o template de contato formal para solicitação de bloqueio com todos os dados disponíveis.
- Se **NÃO**: Explique onde os fundos foram parar, construa o mapa mental ASCII do caminho percorrido, e recomende próximos passos.

Seja específico, cite hashes de transações, endereços e timestamps sempre que disponíveis.
"""

    def _format_transactions(self, transactions: list[dict]) -> str:
        if not transactions:
            return "_Nenhuma transação encontrada. Verifique o endereço da carteira ou configure a Helius API key._"

        lines = []
        for tx in transactions[:15]:
            sig = tx.get("signature", "")
            ts = tx.get("timestamp", 0)
            ts_str = ""
            if ts:
                try:
                    ts_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M UTC")
                except Exception:
                    ts_str = str(ts)

            tx_type = tx.get("type", "UNKNOWN")
            desc = tx.get("description", "")
            fee_payer = tx.get("feePayer", "")

            lines.append(f"\n**TX:** `{sig}` | **Tipo:** {tx_type} | **Hora:** {ts_str}")
            if fee_payer:
                lines.append(f"  - Fee Payer: `{fee_payer}`")
            if desc:
                lines.append(f"  - Descrição: {desc}")

            for t in tx.get("tokenTransfers", []):
                from_a = t.get("fromUserAccount") or "?"
                to_a = t.get("toUserAccount") or "?"
                mint = t.get("mint") or "?"
                amt = t.get("tokenAmount", "?")
                lines.append(f"  - 🪙 Token: `{from_a}` → `{to_a}` | {amt} | Mint: `{mint}`")

            for n in tx.get("nativeTransfers", []):
                from_a = n.get("fromUserAccount") or "?"
                to_a = n.get("toUserAccount") or "?"
                amt_sol = n.get("amount", 0) / 1e9
                lines.append(f"  - ◎ SOL: `{from_a}` → `{to_a}` | {amt_sol:.6f} SOL")

        return "\n".join(lines)

    def _format_graph(self, flow_graph: dict) -> str:
        if not flow_graph:
            return "_Grafo não disponível._"

        summary = flow_graph.get("summary", {})
        nodes = flow_graph.get("nodes", [])
        edges = flow_graph.get("edges", [])
        cex_detected = flow_graph.get("cex_detected", [])
        bridge_detected = flow_graph.get("bridge_detected", [])

        lines = []

        if cex_detected:
            lines.append(f"⚠️ **CEX DETECTADA(S):** {', '.join(cex_detected)}")
        if bridge_detected:
            lines.append(f"🌉 **BRIDGE DETECTADA(S):** {', '.join(bridge_detected)}")

        lines.append(f"\n**Nós no grafo:** {summary.get('total_nodes', 0)} | **Arestas:** {summary.get('total_edges', 0)} | **Profundidade:** {summary.get('max_depth', 0)} hops")

        lines.append("\n**Nós identificados:**")
        for node in nodes:
            cex_flag = " ⚠️ [CEX]" if node.get("is_cex") else ""
            bridge_flag = " 🌉 [BRIDGE]" if node.get("is_bridge") else ""
            lines.append(f"  - [{node['depth']}] `{node['id']}` → **{node.get('label', 'Unknown')}**{cex_flag}{bridge_flag}")

        lines.append("\n**Fluxos detectados:**")
        for edge in edges:
            from_id = edge.get("from", "?")
            to_id = edge.get("to", "?")
            amt = edge.get("amount", "?")
            ts_h = edge.get("timestamp_human", "")
            sig = edge.get("signature") or "?"
            lines.append(f"  - `{from_id}` →→→ `{to_id}` | {amt} | {ts_h} | TX: `{sig}`")

        return "\n".join(lines)

    @staticmethod
    def _detect_cex_mention(content: str) -> bool:
        cex_keywords = [
            "binance", "coinbase", "kraken", "okx", "bybit", "kucoin",
            "ftx", "gate.io", "huobi", "htx", "mexc", "bitfinex", "bitget",
            "cex detectada", "exchange detectada", "fundos na exchange",
            "solicite bloqueio", "solicitar bloqueio",
        ]
        content_lower = content.lower()
        return any(kw in content_lower for kw in cex_keywords)
