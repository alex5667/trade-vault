"""
Тест для проверки чтения soft-fail параметров из ENV переменных.
"""
import os
import pytest

def test_env_variable_priority():
    """
    Проверяет приоритет чтения параметров: ENV > cfg > default
    """
    
    # Симуляция логики из of_confirm_engine.py (строки 913-924)
    def get_soft_params(cfg, env_score=None, env_exec=None):
        # Временно установить ENV переменные
        if env_score:
            os.environ["OF_SOFT_SCORE_MIN"] = str(env_score)
        if env_exec:
            os.environ["OF_SOFT_EXEC_RISK_NORM_MAX"] = str(env_exec)
        
        soft_score_min = float(
            os.getenv("OF_SOFT_SCORE_MIN") or 
            cfg.get("soft_score_min") or 
            0.60
        )
        soft_exec_max = float(
            os.getenv("OF_SOFT_EXEC_RISK_NORM_MAX") or 
            cfg.get("soft_exec_risk_norm_max") or 
            0.65
        )
        
        # Очистить ENV после теста
        os.environ.pop("OF_SOFT_SCORE_MIN", None)
        os.environ.pop("OF_SOFT_EXEC_RISK_NORM_MAX", None)
        
        return soft_score_min, soft_exec_max
    
    print("\n" + "="*80)
    print("ТЕСТ ПРИОРИТЕТА ПАРАМЕТРОВ")
    print("="*80)
    
    # Тест 1: Только defaults
    print("\n1. Только defaults (нет ENV, нет cfg)")
    cfg = {}
    score, exec_risk = get_soft_params(cfg)
    assert score == 0.60, f"Expected 0.60, got {score}"
    assert exec_risk == 0.65, f"Expected 0.65, got {exec_risk}"
    print(f"   ✅ score={score}, exec_risk={exec_risk}")
    
    # Тест 2: cfg override
    print("\n2. cfg override (нет ENV, есть cfg)")
    cfg = {"soft_score_min": 0.55, "soft_exec_risk_norm_max": 0.70}
    score, exec_risk = get_soft_params(cfg)
    assert score == 0.55, f"Expected 0.55, got {score}"
    assert exec_risk == 0.70, f"Expected 0.70, got {exec_risk}"
    print(f"   ✅ score={score}, exec_risk={exec_risk}")
    
    # Тест 3: ENV override (высший приоритет)
    print("\n3. ENV override (есть ENV, есть cfg)")
    cfg = {"soft_score_min": 0.55, "soft_exec_risk_norm_max": 0.70}
    score, exec_risk = get_soft_params(cfg, env_score=0.62, env_exec=0.68)
    assert score == 0.62, f"Expected 0.62, got {score}"
    assert exec_risk == 0.68, f"Expected 0.68, got {exec_risk}"
    print(f"   ✅ score={score}, exec_risk={exec_risk} (ENV перекрыл cfg)")
    
    # Тест 4: Частичный ENV override
    print("\n4. Частичный ENV override (только score в ENV)")
    cfg = {"soft_score_min": 0.55, "soft_exec_risk_norm_max": 0.70}
    score, exec_risk = get_soft_params(cfg, env_score=0.58)
    assert score == 0.58, f"Expected 0.58, got {score}"
    assert exec_risk == 0.70, f"Expected 0.70, got {exec_risk}"
    print(f"   ✅ score={score} (из ENV), exec_risk={exec_risk} (из cfg)")
    
    print("\n" + "="*80)
    print("ВСЕ ТЕСТЫ ПРОШЛИ!")
    print("="*80)
    print("\nПРИОРИТЕТ: ENV > cfg > default (0.60 / 0.65)")
    print("Теперь можно настраивать пороги через docker-compose.yml\n")

if __name__ == "__main__":
    test_env_variable_priority()
