"""
SolTrace - Bot Analisador Rule-Based.
Suporte completo a: CEX, DEX swap, Bridge, PARKED, SPLIT.
"""

import math
from typing import Optional
from datetime import datetime
from collections import defaultdict


class BotAnalyzer:

    def compute_features(self, flow_graph: dict, transactions: list[dict]) -> dict:
        nodes  = flow_graph.get("nodes", [])
        edges  = flow_graph.get("edges", [])
        summary = flow_graph.get("summary", {})

        out_degree: dict[str, int] = defaultdict(int)
        in_sources: dict[str, set] = defaultdict(set)
        for e in edges:
            f = e.get("from", ""); t = e.get("to", "")
            if f: out_degree[f] += 1
            if t and f: in_sources[t].add(f)

        num_splits = sum(1 for c in out_degree.values() if c > 1)
        num_merges = sum(1 for s in in_sources.values() if len(s) > 1)

        amounts = []
        for e in edges:
            a = e.get("amount")
            if a is not None:
                try: amounts.append(float(a))
                except: pass

        ve = 0.0
        if len(amounts) > 1:
            tot = sum(amounts)
            if tot > 0:
                probs = [a / tot for a in amounts]
                ve = -sum(p * math.log2(p) for p in probs if p > 0)

        labeled = sum(1 for n in nodes if (
            n.get("is_cex") or n.get("is_bridge") or n.get("is_dex") or
            n.get("is_defi") or n.get("is_parked") or
            n.get("type") in ("CEX","BRIDGE","DEFI","DEX_SWAP","DEX","VICTIM","PARKED","SPLIT")
            or (n.get("label") and n["label"] not in ("Wallet Destino","Unknown","Desconhecido",""))
        ))

        return {
            "num_hops":             flow_graph.get("summary", {}).get("max_depth", 0),
            "num_wallets":          len(nodes),
            "num_splits":           num_splits,
            "num_merges":           num_merges,
            "value_entropy":        round(ve, 4),
            "has_bridge":           len(flow_graph.get("bridge_detected", [])) > 0,
            "label_coverage_ratio": round(labeled / max(len(nodes), 1), 4),
            "parked_count":         summary.get("parked_count", 0),
            "split_count":          summary.get("split_count", 0),
            "truncated":            summary.get("truncated", False),
        }

    def analyze(self, wallet: str, token: str, amount: float, token_name: Optional[str],
                transactions: list[dict], flow_graph: dict, features: dict) -> dict:

        cex_det     = flow_graph.get("cex_detected", [])
        bridge_det  = flow_graph.get("bridge_detected", [])
        dex_det     = flow_graph.get("dex_detected", [])
        parked_list = flow_graph.get("parked_wallets", [])
        nodes       = flow_graph.get("nodes", [])
        edges       = flow_graph.get("edges", [])
        td          = token_name or token
        now         = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        summary_txt, conf = self._executive_summary(cex_det, bridge_det, dex_det, parked_list, nodes, features, td)
        ascii_map     = self._build_ascii_map(wallet, nodes, edges)
        cex_section   = self._build_cex_section(cex_det, edges, nodes, wallet, td, amount, now)
        dex_section   = self._build_dex_section(dex_det, edges, nodes)
        parked_section = self._build_parked_section(parked_list, nodes, edges)
        split_section  = self._build_split_section(nodes, edges)
        entities_table = self._build_entities_table(nodes)
        timeline      = self._build_timeline(edges, nodes)
        metrics_table = self._build_metrics_table(features, edges, dex_det)
        hypothesis    = self._build_hypothesis(cex_det, bridge_det, dex_det, parked_list, nodes, features)
        next_steps    = self._build_next_steps(cex_det, bridge_det, dex_det, parked_list, features)

        truncated_warn = ""
        if features.get("truncated"):
            truncated_warn = "\n> ⚠️ **Rastreamento interrompido por limite de segurança (80 carteiras).** Use o Agente de IA para análise mais profunda.\n"

        raw_markdown = f"""### 🔍 SUMÁRIO EXECUTIVO
{summary_txt}

**Confiança geral:** {conf} | **Método:** Análise Rule-Based (BOT) | **Data:** {now}
{truncated_warn}
---

### 🗺️ MAPA DO FLUXO DE FUNDOS

```
{ascii_map}
```

---

### 🏦 ANÁLISE CEX
{cex_section}

---

### 🔄 ANÁLISE DEX / SWAP
{dex_section}

---

### 🅿️ CARTEIRAS ESTACIONADAS (PARKED)
{parked_section}

---

### ⚡ ANÁLISE DE SPLITS (1→N)
{split_section}

---

### 📋 ENTIDADES IDENTIFICADAS
{entities_table}

---

### ⏱️ TIMELINE DE EVENTOS
{timeline}

---

### 📊 MÉTRICAS DO GRAFO
{metrics_table}

---

### ⚠️ HIPÓTESE PRINCIPAL + ALTERNATIVAS

**HIPÓTESE PRINCIPAL [CONFIANÇA: {conf}]:**
{hypothesis}

**Hipóteses alternativas:**
- Fundos podem estar aguardando timing para continuar o movimento (padrão de espera)
- Splits podem ser para dificultar correlação entre carteiras
- Token pode ter sido convertido via DEX e saído por rota alternativa

---

### 📬 PRÓXIMOS PASSOS RECOMENDADOS
{next_steps}

---
*Relatório gerado pelo **SolTrace BOT** (análise rule-based, sem custo de IA). \
Para casos com alta complexidade, o sistema escala automaticamente para o Agente de IA.*
"""

        return {
            "raw_markdown": raw_markdown,
            "has_cex": len(cex_det) > 0,
            "graph": flow_graph,
            "metadata": {
                "wallet": wallet, "token": td, "token_mint": token, "amount": amount,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "model": "rule-based-bot", "analysis_method": "bot", "features": features,
                "transactions_analyzed": len(transactions),
                "graph_nodes": len(nodes), "graph_edges": len(edges),
                "cex_detected": cex_det, "bridge_detected": bridge_det,
                "dex_detected": dex_det, "parked_wallets": parked_list,
            },
        }

    # ─────────────────────────────────────────────────────────────────────────

    def _executive_summary(self, cex_det, bridge_det, dex_det, parked_list,
                           nodes, features, td) -> tuple[str, str]:
        parked_count = features.get("parked_count", 0)
        split_count  = features.get("split_count", 0)

        if cex_det:
            return (
                f"Fundos rastreados com **ALTA CONFIANÇA**. "
                f"Detectado depósito em CEX: **{', '.join(cex_det)}**. "
                f"Fluxo de {features['num_hops']} hop(s). Recomenda-se contato imediato para bloqueio.",
                "HIGH"
            )

        parts = []
        if dex_det:
            parts.append(f"Swap via **{', '.join(dex_det)}**")
        if parked_count:
            parts.append(f"**{parked_count} carteira(s) estacionada(s)** aguardando movimento")
        if split_count:
            parts.append(f"**{split_count} split(s)** detectado(s) — fundos divididos em múltiplas carteiras")
        if bridge_det:
            parts.append(f"Bridge cross-chain: **{', '.join(bridge_det)}**")

        if parts:
            descr = "; ".join(parts) + "."
            conf  = "HIGH" if (parked_count and not dex_det) else "MEDIUM"
            return (
                f"Fundos rastreados por {features['num_hops']} hop(s) e {features['num_wallets']} carteiras. "
                f"{descr} Rastreamento completo até os fundos pararem de se mover.",
                conf
            )

        dest = [n for n in nodes if n.get("depth", 0) > 0]
        if dest:
            return (
                f"Fundos rastreados por {features['num_hops']} hop(s) para {len(dest)} carteira(s). "
                f"Nenhuma CEX, DEX ou bridge identificada. Fundos podem estar em carteiras desconhecidas.",
                "MEDIUM"
            )
        return (f"Nenhuma transferência de saída identificada para **{td}**.", "LOW")

    def _build_ascii_map(self, wallet: str, nodes: list, edges: list) -> str:
        lines = [f"[Vítima: {wallet}]"]
        depth_nodes: dict[int, list] = defaultdict(list)
        for n in nodes:
            if n["id"] != wallet: depth_nodes[n.get("depth", 1)].append(n)
        edge_map: dict[str, dict] = {}
        for e in edges:
            t = e.get("to", "")
            if t and t not in edge_map: edge_map[t] = e

        for depth in sorted(depth_nodes.keys()):
            for node in depth_nodes[depth]:
                nid    = node["id"]
                label  = node.get("label", "Desconhecido")
                indent = "    " * (depth - 1)
                if   node.get("is_cex"):    icon = " ⚠️ [CEX]"
                elif node.get("is_bridge"): icon = " 🌉 [BRIDGE]"
                elif node.get("is_dex") or node.get("type") == "DEX_SWAP": icon = " 🔄 [DEX SWAP]"
                elif node.get("is_parked"): icon = " 🅿️ [PARKED]"
                elif node.get("is_split"):  icon = f" ⚡ [SPLIT→{node.get('split_count','N')}]"
                else: icon = ""

                e = edge_map.get(nid, {})
                amt    = e.get("amount", "?")
                sig    = e.get("signature") or "?"
                t_type = e.get("transfer_type", "TRANSFER")
                dex_n  = e.get("dex_name", "")
                out_m  = e.get("output_mint", "")

                if t_type == "DEX_SWAP" and dex_n:
                    lines.append(f"{indent}        ↓ SWAP via {dex_n} | entrada: {amt}")
                    if out_m: lines.append(f"{indent}        ↳ token saída: {out_m[:30]}...")
                else:
                    lines.append(f"{indent}        ↓ {amt}")
                lines.append(f"{indent}        TX: {sig}")
                lines.append(f"{indent}[{label}: {nid}]{icon}")

        return "\n".join(lines) if len(lines) > 1 else f"[Vítima: {wallet}]\n    (sem transferências identificadas)"

    def _build_cex_section(self, cex_det, edges, nodes, wallet, td, amount, now) -> str:
        if not cex_det:
            dest   = {e.get("to") for e in edges}
            source = {e.get("from") for e in edges}
            finals = dest - source - {wallet}
            fn = [n for n in nodes if n["id"] in finals and not n.get("is_dex") and not n.get("is_bridge")]
            if fn:
                addrs = "\n".join(f"  - `{n['id']}` — {n.get('label','?')}" for n in fn[:8])
                return f"Nenhuma CEX identificada.\n\n**Endereços finais detectados:**\n{addrs}"
            return "Nenhuma CEX identificada."

        secs = []
        for cex in cex_det:
            cn = [n for n in nodes if n.get("is_cex") and cex.lower() in n.get("label","").lower()]
            for c in cn:
                de = [e for e in edges if e.get("to") == c["id"]]
                for d in de[:1]:
                    sig = d.get("signature","N/A"); ts = d.get("timestamp_human","?"); amt = d.get("amount", amount)
                    secs.append(f"""**Exchange: {cex}**
- Endereço: `{c['id']}`
- TX: `{sig}`
- Timestamp: {ts}
- Valor: {amt} {td}
- **STATUS: SOLICITE BLOQUEIO IMEDIATAMENTE**

```
Assunto: Solicitação Urgente de Bloqueio — Roubo Blockchain

Para: Compliance / {cex} | Data: {now}

Fundos roubados detectados em vossa exchange.
- Vítima: {wallet}
- Token: {td} | Valor: {amount}
- TX depósito: {sig}
- Endereço depósito: {c['id']}
- Timestamp: {ts}

Solicito: 1) Bloqueio preventivo 2) Preservação KYC 3) Cooperação legal
```""")
        return "\n\n".join(secs) if secs else f"CEX detectada: {', '.join(cex_det)} (sem detalhes no grafo)"

    def _build_dex_section(self, dex_det, edges, nodes) -> str:
        if not dex_det:
            return "Nenhum swap em DEX detectado."
        lines = [f"⚠️ **Swap detectado: {', '.join(dex_det)}**\n",
                 "O token original pode ter sido convertido, dificultando rastreamento direto.\n"]
        for e in edges:
            if e.get("transfer_type") == "DEX_SWAP" or e.get("dex_name"):
                out_m = e.get("output_mint") or "desconhecido"
                out_a = e.get("output_amount")
                lines += [
                    f"**Swap:** `{e.get('from','?')[:30]}...`",
                    f"- DEX: **{e.get('dex_name','?')}**",
                    f"- Entrada: {e.get('amount','?')} | TX: `{(e.get('signature') or '?')[:50]}...`",
                    f"- Token saída: `{out_m[:40]}` | Valor saída: {out_a or 'desconhecido'}",
                    f"- Timestamp: {e.get('timestamp_human','?')}\n",
                ]
        lines += [
            "**Para rastrear pós-swap:**",
            "1. Abrir TX no **[Solscan](https://solscan.io)** e confirmar token de saída",
            "2. Iniciar nova investigação com o token de saída e carteira receptora",
            "3. Se saída for USDC/USDT/SOL → alto risco de depósito em CEX",
        ]
        return "\n".join(lines)

    def _build_parked_section(self, parked_list, nodes, edges) -> str:
        parked_nodes = [n for n in nodes if n.get("is_parked")]
        if not parked_nodes:
            return "Nenhuma carteira estacionada detectada — todos os fundos estão em movimento ou chegaram a um destino final."

        lines = [
            f"🅿️ **{len(parked_nodes)} carteira(s) estacionada(s) detectada(s).**",
            "Estas carteiras receberam fundos mas não realizaram nenhuma transferência de saída.",
            "Padrão típico de **espera de timing** antes de continuar a movimentação.\n",
        ]
        in_edges = {e.get("to"): e for e in edges}
        for n in parked_nodes:
            nid  = n["id"]
            ie   = in_edges.get(nid, {})
            amt  = ie.get("amount", "?")
            ts   = ie.get("timestamp_human", "?")
            sig  = ie.get("signature") or "?"
            depth = n.get("depth", "?")
            lines += [
                f"**Carteira:** `{nid}`",
                f"- Hop: {depth} | Valor recebido: {amt} | Recebido em: {ts}",
                f"- TX de entrada: `{sig[:60]}...`",
                f"- **Monitorar:** https://solscan.io/account/{nid}\n",
            ]
        lines += [
            "⚠️ **Ação recomendada:** Monitorar estas carteiras continuamente.",
            "Quando movimentarem, executar nova investigação com o hash da nova TX.",
        ]
        return "\n".join(lines)

    def _build_split_section(self, nodes, edges) -> str:
        split_nodes = [n for n in nodes if n.get("is_split")]
        if not split_nodes:
            return "Nenhum split detectado — fluxo linear sem dispersão significativa."

        lines = [
            f"⚡ **{len(split_nodes)} split(s) detectado(s).**",
            "Carteiras que dividiram os fundos em múltiplos destinos simultâneos.",
            "Técnica usada para dificultar correlação e aumentar complexidade do rastreamento.\n",
        ]
        for n in split_nodes:
            nid = n["id"]
            sc  = n.get("split_count", "?")
            out_edges = [e for e in edges if e.get("from") == nid]
            lines += [f"**Carteira split:** `{nid}` → {sc} destinos"]
            for e in out_edges[:10]:
                to   = e.get("to", "?")
                amt  = e.get("amount", "?")
                to_n = next((x for x in nodes if x["id"] == to), {})
                to_l = to_n.get("label", "Wallet")
                to_t = to_n.get("type", "")
                lines.append(f"  ↳ `{to[:30]}...` ({to_l}/{to_t}) | {amt}")
            lines.append("")
        return "\n".join(lines)

    def _build_entities_table(self, nodes: list) -> str:
        header = "| Endereço | Tipo | Label | Risco | Hop |\n|----------|------|-------|-------|-----|\n"
        rows = []
        for n in nodes:
            addr  = n["id"]
            ntype = n.get("type", "WALLET")
            label = n.get("label", "Desconhecido")
            if n.get("is_cex"):     risk = "HIGH ⚠️"
            elif n.get("is_bridge"):risk = "MEDIUM 🌉"
            elif n.get("is_dex"):   risk = "LOW 🔄"; ntype = "DEX_SWAP 🔄"
            elif n.get("is_parked"):risk = "⚡ MONITORAR 🅿️"; ntype = "PARKED 🅿️"
            elif n.get("is_split"): risk = "MEDIUM ⚡"; ntype = f"SPLIT({n.get('split_count','?')}) ⚡"
            else: risk = "LOW"
            rows.append(f"| `{addr[:40]}` | {ntype} | {label} | {risk} | {n.get('depth',0)} |")
        return header + "\n".join(rows) if rows else header + "| — | — | — | — | — |"

    def _build_timeline(self, edges: list, nodes: list) -> str:
        node_labels = {n["id"]: n.get("label","?") for n in nodes}
        events = []
        for e in edges:
            ts   = e.get("timestamp") or 0
            tsh  = e.get("timestamp_human","?")
            sig  = e.get("signature") or "?"
            frm  = e.get("from") or "?"
            to   = e.get("to") or "?"
            amt  = e.get("amount","?")
            lbl  = node_labels.get(to,"?")
            tt   = e.get("transfer_type","TRANSFER")
            dexn = e.get("dex_name","")
            outm = e.get("output_mint","")
            if tt == "DEX_SWAP" and dexn:
                action = f"SWAP via {dexn} | entrada: {amt}"
                if outm: action += f" | saída: {outm[:25]}..."
            else:
                action = f"Valor: {amt}"
            events.append((ts, f"- **{tsh}**\n  - De: `{frm}`\n  - Para: `{to}` ({lbl})\n  - {action}\n  - TX: `{sig}`"))
        events.sort(key=lambda x: x[0])
        return "\n".join(e[1] for e in events) if events else "_Nenhum evento encontrado._"

    def _build_metrics_table(self, features: dict, edges: list, dex_det: list) -> str:
        tv  = sum(float(e.get("amount",0) or 0) for e in edges if e.get("amount") is not None)
        dex = f"Sim 🔄 ({', '.join(dex_det)})" if dex_det else "Não"
        pc  = features.get("parked_count", 0)
        sc  = features.get("split_count", 0)
        return (
            f"| Métrica | Valor |\n|---------|-------|\n"
            f"| Hops rastreados | {features['num_hops']} |\n"
            f"| Carteiras únicas | {features['num_wallets']} |\n"
            f"| Fluxos (arestas) | {len(edges)} |\n"
            f"| Splits (1→N) | {sc} |\n"
            f"| Merges (N→1) | {features['num_merges']} |\n"
            f"| Carteiras Parked | {pc} |\n"
            f"| Entropia de valores | {features['value_entropy']:.4f} bits |\n"
            f"| Cobertura de labels | {features['label_coverage_ratio']*100:.1f}% |\n"
            f"| Bridge/cross-chain | {'Sim 🌉' if features['has_bridge'] else 'Não'} |\n"
            f"| DEX swap detectado | {dex} |\n"
            f"| Volume total rastreado | {tv:.6f} |\n"
            f"| Rastreamento truncado | {'Sim ⚠️' if features.get('truncated') else 'Não'} |\n"
        )

    def _build_hypothesis(self, cex_det, bridge_det, dex_det, parked_list, nodes, features) -> str:
        pc = features.get("parked_count", 0)
        sc = features.get("split_count", 0)
        if cex_det:
            return f"Fundos depositados em **{', '.join(cex_det)}** — rastreabilidade via KYC da exchange."
        if pc and sc:
            return (f"Padrão de **lavagem estruturada**: fundos divididos em {sc} split(s) e "
                    f"{pc} carteira(s) estacionada(s) aguardando timing para continuar. "
                    f"Técnica clássica de mixing manual — monitoramento contínuo necessário.")
        if pc:
            return (f"Fundos estão em {pc} carteira(s) estacionada(s) — padrão de **espera de timing**. "
                    f"Fundos podem ser movidos a qualquer momento. Monitoramento imediato recomendado.")
        if sc:
            return (f"Fundos divididos via {sc} split(s) em múltiplas carteiras — técnica de **ofuscação por dispersão**. "
                    f"Rastrear cada branch individualmente.")
        if dex_det:
            return (f"Swap via **{', '.join(dex_det)}** — token original convertido. "
                    f"Verificar token de saída e continuar rastreamento.")
        if bridge_det:
            return f"Fundos encaminhados para bridge: **{', '.join(bridge_det)}**. Rastrear na chain de destino."
        return "Fluxo rastreado até término. Sem CEX, DEX ou bridge identificados."

    def _build_next_steps(self, cex_det, bridge_det, dex_det, parked_list, features) -> str:
        pc = features.get("parked_count", 0)
        sc = features.get("split_count", 0)
        if cex_det:
            return "\n".join([
                f"1. Enviar template de contato para **{', '.join(cex_det)}** solicitando bloqueio preventivo.",
                "2. Preservar todos os hashes de TX como evidência forense.",
                "3. Registrar BO citando endereços e TXs identificados.",
                "4. Monitorar o endereço de depósito para novas movimentações.",
                "5. Acionar autoridades locais e/ou INTERPOL conforme jurisdição.",
            ])
        steps = []
        if pc:
            steps.append(f"1. 🅿️ **Monitorar as {pc} carteiras estacionadas** — configurar alertas em [Solscan](https://solscan.io) ou [SolanaFM](https://solana.fm).")
            steps.append(f"2. Quando movimentarem, executar nova investigação SolTrace com o hash da nova TX.")
        if sc:
            n = len(steps) + 1
            steps.append(f"{n}. ⚡ **Rastrear cada branch do split individualmente** — iniciar investigações separadas por carteira.")
        if dex_det:
            n = len(steps) + 1
            steps += [
                f"{n}. 🔄 Abrir TX de swap no Solscan e identificar o token de saída.",
                f"{n+1}. Iniciar nova investigação com o **token de saída** e a **carteira receptora**.",
                f"{n+2}. Se saída for USDC/USDT/SOL → risco alto de depósito em CEX.",
            ]
        if bridge_det:
            n = len(steps) + 1
            steps.append(f"{n}. 🌉 Rastrear fundos na chain de destino de **{', '.join(bridge_det)}**.")
        if not steps:
            steps = [
                "1. Monitorar carteiras identificadas para novas movimentações.",
                "2. Verificar endereços no Solscan para identificar entidades.",
                "3. Aguardar movimentação e executar nova investigação.",
            ]
        if features.get("truncated"):
            steps.append(f"{len(steps)+1}. ⚠️ Rastreamento foi interrompido por limite de segurança — use o **Agente de IA** para análise mais profunda.")
        return "\n".join(steps)
