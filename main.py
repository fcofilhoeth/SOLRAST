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
    version="1.0.0",
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
    token: str                          # mint address OU símbolo (ex: "USDC", "SOL")
    amount: float
    token_name: Optional[str] = None    # nome amigável opcional
    tx_hash: Optional[str] = None       # hash da TX do roubo (opcional, aumenta precisão)
    max_hops: Optional[int] = Field(default=10, ge=1, le=15)  # profundidade do rastreamento


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
        "groq_configured": bool(os.getenv("GROQ_API_KEY")),
        "helius_configured": bool(os.getenv("HELIUS_API_KEY")),
        "model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        "routing": {
            "bot_available": True,
            "ai_available": bool(os.getenv("GROQ_API_KEY")),
            "description": "BOT handles simple flows (≤5 hops, no splits/bridges). AI (Groq/Llama) handles complex cases.",
        },
    }


@app.post("/api/investigate")
async def investigate(req: InvestigationRequest):
    """
    Inicia investigação forense on-chain para uma carteira Solana hackeada.
    Retorna relatório completo com mapa de fluxo e análise de CEX.
    """
    # Validações básicas
    wallet = req.wallet.strip()
    token = req.token.strip()

    if len(wallet) < 32:
        raise HTTPException(status_code=400, detail="Endereço de carteira inválido. Deve ser um endereço Solana válido (base58).")

    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Quantidade deve ser maior que zero.")

    try:
        fetcher = SolanaFetcher()
        orchestrator = InvestigationOrchestrator()

        # Passo 1: Coletar dados on-chain
        tx_hash = (req.tx_hash or "").strip() or None

        print(f"\n[SolTrace] Iniciando investigação")
        print(f"  Wallet:  {wallet}")
        print(f"  Token:   {token}")
        print(f"  Amount:  {req.amount}")
        if tx_hash:
            print(f"  TX Hash: {tx_hash}")

        transactions = await fetcher.get_wallet_transactions(wallet, limit=50)
        print(f"  Transações encontradas: {len(transactions)}")

        # Passo 2: Rastrear fluxo de fundos (sempre via BOT/regras)
        flow_graph = await fetcher.trace_token_flow(
            wallet=wallet,
            token=token,
            amount=req.amount,
            transactions=transactions,
            max_hops=min(req.max_hops or 10, 15),
            tx_hash=tx_hash,
        )
        print(f"  Nós no grafo: {len(flow_graph.get('nodes', []))}")
        print(f"  CEX detectadas: {flow_graph.get('cex_detected', [])}")

        # Passo 3: Orquestrador decide BOT vs IA com base nas features do grafo
        report = await orchestrator.route(
            wallet=wallet,
            token=token,
            amount=req.amount,
            token_name=req.token_name,
            transactions=transactions,
            flow_graph=flow_graph,
        )

        method = report.get("metadata", {}).get("analysis_method", "unknown")
        print(f"[SolTrace] Investigação concluída! Método: {method}")
        return {"status": "success", "report": report}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[SolTrace] ERRO: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno na investigação: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Static files (frontend) — deve vir DEPOIS das rotas da API
# ─────────────────────────────────────────────────────────────────────────────

if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  SolTrace - On-Chain Investigator v1.0")
    print("  Solana Blockchain Forensics Agent")
    print("=" * 50)
    print("  Servidor: http://localhost:8000")
    print("  API Docs: http://localhost:8000/docs")
    print("  Health:   http://localhost:8000/health")
    print("=" * 50)
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_excludes=["frontend/*"],
    )
