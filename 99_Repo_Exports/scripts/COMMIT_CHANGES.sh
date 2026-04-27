#!/bin/bash

# ═══════════════════════════════════════════════════════════════════
#  Git Commit - Go Gateway Integration
# ═══════════════════════════════════════════════════════════════════

set -e

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║     📝 КОММИТ ИЗМЕНЕНИЙ GO-GATEWAY ИНТЕГРАЦИИ                 ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

cd /home/alex/front/trade/scanner_infra

# Добавить измененные файлы go-gateway
echo "✅ Добавление go-gateway файлов..."
git add go-gateway/Dockerfile
git add go-gateway/go.mod
git add go-gateway/go.sum
git add go-gateway/main.go
git add go-gateway/internal/

# Добавить новую документацию
echo "✅ Добавление документации..."
git add GO_GATEWAY_INTEGRATION_SUCCESS.md
git add GO_GATEWAY_FILES_CHANGED.md
git add QUICK_START.md
git add START_SYSTEM.sh
git add COMMIT_CHANGES.sh

# Показать статус
echo ""
echo "📊 Файлы для коммита:"
git status --short | grep -E "^(A|M)" | head -20

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Выполнить коммит? (y/n)                                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
read -p "Ваш выбор: " choice

if [ "$choice" = "y" ] || [ "$choice" = "Y" ]; then
    echo ""
    echo "📝 Создание коммита..."
    
    git commit -m "fix: Complete go-gateway integration and resolve build errors

✨ Проблемы:
- Missing go.sum entries for redis/go-redis/v9
- Internal packages not found in Docker build
- Missing types: RedisListCandleRepo, ATRService, SymbolSpecsLoader
- Missing function: NewPaperEngine
- Incorrect paperMode declaration order

✅ Исправления:

1. Dependencies (go.sum)
   - Run 'go mod tidy' to resolve all dependencies
   - Added redis/go-redis/v9, gorilla/websocket, google/uuid

2. Dockerfile
   - Added 'COPY internal/ ./internal/' for internal packages

3. internal/runtime/atr.go (+59 lines)
   - Added RedisListCandleRepo struct
   - Added ATRService struct with HTTP handlers
   - Added NewATRService() constructor
   - Added RegisterHTTPHandlers() method
   - Added /runtime/atr endpoint

4. internal/risk/symbol_specs.go (+37 lines)
   - Extended SymbolSpecs with: Symbol, LotStep, MinLot, MaxLot,
     ATRPeriod, ATRSLMult, ATRTPMults
   - Added SymbolSpecsLoader type
   - Added NewSymbolSpecsLoader() constructor
   - Added Get() method for cached specs loading

5. internal/paper/paper_pnl.go (+124 lines)
   - Added PaperEngine struct with Redis integration
   - Added NewPaperEngine() constructor
   - Added RegisterHTTPHandlers() method
   - Added HTTP handlers: /paper/positions, /paper/summary,
     /paper/open, /paper/close

6. main.go
   - Moved PaperStatus and paperMode declarations before usage
   - Removed duplicate declarations

📊 Результаты:
- ✅ Code compiles without errors
- ✅ Docker image builds successfully
- ✅ All services running and healthy
- ✅ API endpoints tested and working
- ✅ Telegram integration confirmed
- ✅ Paper trading functional
- ✅ System integration verified

📚 Documentation:
- GO_GATEWAY_INTEGRATION_SUCCESS.md - complete report
- GO_GATEWAY_FILES_CHANGED.md - file changes list
- QUICK_START.md - quick start guide
- START_SYSTEM.sh - auto-start script

🚀 Status: Production Ready"

    echo ""
    echo "✅ Коммит создан успешно!"
    echo ""
    echo "Для отправки в удалённый репозиторий выполните:"
    echo "  git push origin main"
    echo ""
else
    echo ""
    echo "❌ Коммит отменён."
    echo ""
fi

