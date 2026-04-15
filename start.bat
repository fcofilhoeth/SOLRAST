@echo off
echo ============================================
echo   SolTrace - On-Chain Investigator v1.0
echo   Solana Blockchain Forensics Agent
echo ============================================
echo.
echo Verificando configuracao...

if not exist ".env" (
    echo AVISO: Arquivo .env nao encontrado!
    echo Copie o .env.example para .env e configure suas chaves.
    pause
    exit /b 1
)

echo Iniciando servidor...
echo Acesse: http://localhost:8000
echo.
python main.py
pause
