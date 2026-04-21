"""
SolTrace - MVP de Investigação On-Chain Solana
Backend FastAPI
"""

import os
import json
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import uvicorn
from dotenv import load_dotenv

load_dotenv()

from orchestrator import InvestigationOrchestrator
from solana_fetcher import SolanaFetcher

# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="SolTrace — Investigador On-Chain Solana",
    description="Agente forense blockchain para rastreamento de fundos roubados na Solana",
    version="1.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).parent / "frontend"

# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────

class InvestigationRequest(BaseModel):
    wallet: str
    token: str                       # mint address OU símbolo (ex: "USDC", "SOL")
    amount: float
    token_name: Optional[str] = None # nome amigável opcional
    tx_hash: str                     # hash da TX do roubo (obrigatório)
    max_hops: Optional[int] = Field(
        default=20, ge=1, le=20,
        description="Profundidade máxima de rastreamento. O sistema para automaticamente quando os fundos param de se mover."
    )


class InvestigationResponse(BaseModel):
    status: str
    report: dict


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def root():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse({"status": "SolTrace API running. Frontend não encontrado."})


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "openai_configured": bool(os.getenv("OPENAI_API_KEY")),
        "helius_configured": bool(os.getenv("HELIUS_API_KEY")),
        "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        "version": "1.1.0",
        "routing": {
            "bot_available": True,
            "ai_available": bool(os.getenv("OPENAI_API_KEY")),
            "description": (
                "BOT rastreia fundos até pararem de se mover, detecta PARKED/SPLIT/DEX/CEX. "
                "IA (OpenAI) analisa casos com alta complexidade."
            ),
        },
        "limits": {
            "max_bfs_wallets": 80,
            "max_outgoing_per_node": 15,
            "max_depth": 20,
        },
    }


@app.post("/api/investigate")
async def investigate(req: InvestigationRequest):
    """
    Inicia investigação forense on-chain para uma carteira Solana hackeada.
    Rastreia fundos até pararem de se mover.
    Detecta: CEX, DEX swap (com continuação pós-swap), Bridge,
             carteiras estacionadas (PARKED) e splits (1→N).
    """
    wallet   = req.wallet.strip()
    token    = req.token.strip()
    tx_hash  = (req.tx_hash or "").strip() or None

    if len(wallet) < 32:
        raise HTTPException(status_code=400, detail="Endereço de carteira inválido (mínimo 32 caracteres base58).")
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Quantidade deve ser maior que zero.")
    if not tx_hash:
        raise HTTPException(status_code=400, detail="Hash da transação do roubo é obrigatório para garantir precisão no rastreamento.")

    try:
        fetcher      = SolanaFetcher()
        orchestrator = InvestigationOrchestrator()

        print(f"\n[SolTrace] ═══ Iniciando investigação ═══")
        print(f"  Wallet:  {wallet}")
        print(f"  Token:   {token}")
        print(f"  Amount:  {req.amount}")
        print(f"  TX Hash: {tx_hash}")
        print(f"  Max depth: {req.max_hops or 20}")

        # Passo 1: Busca transações da carteira vítima
        transactions = await fetcher.get_wallet_transactions(wallet, limit=50)
        print(f"  Transações encontradas: {len(transactions)}")

        # Passo 2: Rastreia fluxo até os fundos pararem
        flow_graph = await fetcher.trace_token_flow(
            wallet=wallet,
            token=token,
            amount=req.amount,
            transactions=transactions,
            max_hops=min(req.max_hops or 20, 20),
            tx_hash=tx_hash,
        )

        summary = flow_graph.get("summary", {})
        print(f"  ─── Resultado do rastreamento ───")
        print(f"  Nós: {len(flow_graph.get('nodes', []))} | Arestas: {len(flow_graph.get('edges', []))}")
        print(f"  CEX: {flow_graph.get('cex_detected', [])} | DEX: {flow_graph.get('dex_detected', [])}")
        print(f"  Parked: {summary.get('parked_count', 0)} | Splits: {summary.get('split_count', 0)}")
        print(f"  Profundidade máx: {summary.get('max_depth', 0)} | Truncado: {summary.get('truncated', False)}")

        # Passo 3: Orquestrador decide BOT vs IA
        report = await orchestrator.route(
            wallet=wallet,
            token=token,
            amount=req.amount,
            token_name=req.token_name,
            transactions=transactions,
            flow_graph=flow_graph,
        )

        method = report.get("metadata", {}).get("analysis_method", "unknown")
        print(f"[SolTrace] ═══ Concluído! Método: {method} ═══\n")
        return {"status": "success", "report": report}

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[SolTrace] ERRO: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Erro interno na investigação: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Static files — deve vir DEPOIS das rotas da API
# ─────────────────────────────────────────────────────────────────────────────

if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  SolTrace v1.1 - On-Chain Investigator")
    print("  Rastreia fundos até pararem de se mover")
    print("=" * 55)
    print("  Servidor: http://localhost:8000")
    print("  API Docs: http://localhost:8000/docs")
    print("  Health:   http://localhost:8000/health")
    print("=" * 55)
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_excludes=["frontend/*"],
    )
