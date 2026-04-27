# ✅ Integration Checklist - Cost Edge Gate + Enhanced Confidence

**Date**: December 27, 2025  
**Status**: Ready for deployment

---

## 📋 Pre-Deployment Checklist

### ✅ Code Quality
- [x] All new modules created with full docstrings
- [x] All code comments preserved
- [x] No linter errors (verified)
- [x] Type hints added where applicable
- [x] Fail-open behavior implemented

### ✅ Configuration
- [x] YAML anchor created in docker-compose.yml
- [x] All ENV variables documented
- [x] Symbol-specific overrides configured
- [x] Default values set appropriately

### ✅ Integration
- [x] Filters initialized in `crypto_orderflow_init.py`
- [x] Filters applied in `crypto_orderflow_handler.py`
- [x] Proper error handling added
- [x] Detailed logging implemented

### ✅ Documentation
- [x] Full integration guide created (`COST_EDGE_CONFIDENCE_INTEGRATION.md`)
- [x] Quick summary created (`INTEGRATION_SUMMARY_2025-12-27.md`)
- [x] Checklist created (this file)

---

## 🔬 Testing Commands

### 1. Verify Docker Compose Syntax
```bash
cd /home/alex/front/trade/scanner_infra
docker-compose config > /tmp/compose-check.yml
echo $?  # Should be 0
```

### 2. Check YAML Anchor Expansion
```bash
docker-compose config | grep -A 5 "crypto-orderflow-service:" | grep EDGE_COST_GATE_ENABLED
# Should output: - EDGE_COST_GATE_ENABLED=1
```

### 3. Verify Python Imports
```bash
cd /home/alex/front/trade/scanner_infra/python-worker
python3 -c "from handlers.crypto_orderflow.core.cost_edge_gate import CostEdgeGate; print('✅ CostEdgeGate import OK')"
python3 -c "from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdFilter; print('✅ ConfidenceThresholdFilter import OK')"
```

### 4. Test ENV Variable Parsing
```bash
export EDGE_COST_GATE_ENABLED=1
export EDGE_COST_K=4.0
export EDGE_COST_K_BTCUSDT=5.0
export MIN_CONF_BTCUSDT=75

cd /home/alex/front/trade/scanner_infra/python-worker
python3 << 'EOF'
from handlers.crypto_orderflow.core.cost_edge_gate import CostEdgeConfig
from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig

cost_cfg = CostEdgeConfig.from_env()
conf_cfg = ConfidenceThresholdConfig.from_env()

print(f"✅ Cost gate enabled: {cost_cfg.enabled}")
print(f"✅ Default K: {cost_cfg.default_cost_k}")
print(f"✅ BTC K: {cost_cfg.symbol_cost_k.get('BTCUSDT', 'NOT SET')}")
print(f"✅ BTC min conf: {conf_cfg.min_conf_by_symbol.get('BTCUSDT', 'NOT SET')}")
EOF
```

---

## 🚀 Deployment Steps

### Step 1: Backup Current Configuration
```bash
cd /home/alex/front/trade/scanner_infra
cp docker-compose.yml docker-compose.yml.backup-$(date +%Y%m%d_%H%M%S)
echo "✅ Backup created"
```

### Step 2: Validate Services
```bash
# Check if services are defined correctly
docker-compose config --services | grep crypto-orderflow
# Should show:
#   crypto-orderflow-service
#   crypto-orderflow-service-2
```

### Step 3: Build Images (if needed)
```bash
# Only if Dockerfile.gpu was modified
docker-compose build crypto-orderflow-service crypto-orderflow-service-2
```

### Step 4: Restart Services
```bash
# Stop services
docker-compose stop crypto-orderflow-service crypto-orderflow-service-2

# Start with new configuration
docker-compose up -d crypto-orderflow-service crypto-orderflow-service-2

# Verify containers are running
docker-compose ps | grep crypto-orderflow
```

### Step 5: Monitor Startup
```bash
# Watch logs for initialization
docker-compose logs -f --tail=50 crypto-orderflow-service | grep -E "(Cost|Confidence|veto)"

# Wait for successful startup (look for "ready" or "listening" messages)
```

---

## 🔍 Post-Deployment Verification

### 1. Check ENV Variables in Container
```bash
docker-compose exec crypto-orderflow-service env | grep EDGE_COST
# Should show all EDGE_COST_* variables

docker-compose exec crypto-orderflow-service env | grep MIN_CONF
# Should show all MIN_CONF_* variables
```

### 2. Monitor Veto Decisions
```bash
# Real-time veto monitoring (let run for 5-10 minutes)
docker-compose logs -f crypto-orderflow-service | grep -E "(Cost edge veto|Confidence threshold veto)"
```

### 3. Check Veto Counters
```bash
# Look for periodic stats in logs
docker-compose logs --since 30m crypto-orderflow-service | grep "veto_total"
```

### 4. Verify Symbol-Specific Filtering
```bash
# Check that BTC/ETH have different thresholds
docker-compose logs --since 1h crypto-orderflow-service | grep "BTCUSDT.*veto" | head -5
docker-compose logs --since 1h crypto-orderflow-service | grep "ETHUSDT.*veto" | head -5
```

---

## 📊 Success Criteria

### Immediate (within 1 hour)
- [ ] No Python import errors
- [ ] Services start successfully
- [ ] ENV variables loaded correctly
- [ ] Veto decisions logged with details

### Short-term (within 24 hours)
- [ ] Veto rate between 10-40% (expected range)
- [ ] BTC/ETH veto rate > other symbols (stricter thresholds)
- [ ] No unexpected crashes or errors
- [ ] Cost edge vetoes include valid edge estimates

### Medium-term (within 1 week)
- [ ] Reduced number of losing trades
- [ ] Higher win rate on major pairs
- [ ] Improved risk/reward ratio
- [ ] No significant decrease in total signal count (unless desired)

---

## 🔧 Troubleshooting

### Issue: Services won't start
```bash
# Check for syntax errors
docker-compose config

# Check container logs
docker-compose logs crypto-orderflow-service | tail -100

# Verify Python path
docker-compose exec crypto-orderflow-service python --version
```

### Issue: ENV variables not loaded
```bash
# Verify YAML anchor syntax
docker-compose config | grep -A 10 "x-crypto-of-env"

# Check if anchor is referenced
docker-compose config | grep "crypto_of_env"
```

### Issue: Import errors
```bash
# Verify files exist
docker-compose exec crypto-orderflow-service ls -la /app/handlers/crypto_orderflow/core/

# Check Python can find modules
docker-compose exec crypto-orderflow-service python -c "import sys; print('\n'.join(sys.path))"
```

### Issue: No veto logs appearing
```bash
# Check if filters are enabled
docker-compose exec crypto-orderflow-service env | grep EDGE_COST_GATE_ENABLED

# Verify log level
docker-compose exec crypto-orderflow-service env | grep LOG_LEVEL

# Check if signals are being generated at all
docker-compose logs --since 10m crypto-orderflow-service | grep -i signal
```

---

## 🎛️ Tuning Guide

### If too many vetoes (< 50% signals passing)
```yaml
# Reduce cost multiplier
- EDGE_COST_K=3.0
- EDGE_COST_K_BTCUSDT=4.0

# Lower confidence thresholds
- MIN_CONF_BTCUSDT=72
- MIN_CONF_FACTOR_BTCUSDT=0.50
```

### If too few vetoes (> 90% signals passing)
```yaml
# Increase cost multiplier
- EDGE_COST_K=5.0
- EDGE_COST_K_BTCUSDT=6.0

# Raise confidence thresholds
- MIN_CONF_BTCUSDT=78
- MIN_CONF_FACTOR_BTCUSDT=0.60
```

### If edge estimates unavailable
```bash
# Check logs for edge_source
docker-compose logs crypto-orderflow-service | grep "edge_source=none"

# Verify TP1/RR calculation in signals
docker-compose logs crypto-orderflow-service | grep -E "(tp1|rr|atr)"
```

---

## 📈 Metrics to Track

### Daily
- Total signals generated
- Veto rate (overall)
- Veto rate per symbol (BTC, ETH, others)
- Edge ratio distribution

### Weekly
- Win rate before/after integration
- Average R:R before/after
- Number of trades below costs (should be ~0)
- Profit factor trend

### Monthly
- Overall profitability impact
- Sharpe ratio change
- Maximum drawdown comparison
- Signal quality score

---

## 🔄 Rollback Plan

### Quick Disable (no restart)
```bash
# Set ENV override (requires code support)
# OR disable via config hot-reload if supported
```

### Full Rollback
```bash
# Stop services
docker-compose stop crypto-orderflow-service crypto-orderflow-service-2

# Restore backup
cp docker-compose.yml.backup-YYYYMMDD_HHMMSS docker-compose.yml

# Restart with old config
docker-compose up -d crypto-orderflow-service crypto-orderflow-service-2
```

### Partial Disable (keep code, disable filters)
```yaml
# In docker-compose.yml, add to environment:
- EDGE_COST_GATE_ENABLED=0

# Then restart:
docker-compose restart crypto-orderflow-service crypto-orderflow-service-2
```

---

## 📞 Support References

### Log Locations
- Container logs: `docker-compose logs crypto-orderflow-service`
- Volume logs: `/home/alex/front/trade/scanner_infra/logs/`

### Key Files
- Configuration: `docker-compose.yml` (lines 1-213, 1750-1777, 1778-1809)
- Cost Edge Gate: `python-worker/handlers/crypto_orderflow/core/cost_edge_gate.py`
- Confidence Filter: `python-worker/handlers/crypto_orderflow/core/confidence_threshold.py`
- Integration: `python-worker/handlers/crypto_orderflow_handler.py` (lines 1175-1241)

### Documentation
- Full Guide: `COST_EDGE_CONFIDENCE_INTEGRATION.md`
- Quick Summary: `INTEGRATION_SUMMARY_2025-12-27.md`

---

## ✅ Final Sign-Off

**Integration Status**: ✅ Complete  
**Code Quality**: ✅ Verified (no linter errors)  
**Documentation**: ✅ Complete  
**Ready for Deployment**: ✅ Yes

**Deployed by**: _____________  
**Date**: _____________  
**Verified by**: _____________

---

*Checklist prepared by: Claude (Anthropic AI)*  
*Date: December 27, 2025*

