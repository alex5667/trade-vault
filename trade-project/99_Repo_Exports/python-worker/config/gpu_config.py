# config/gpu_config.py
"""
Конфигурация GPU для ускорения вычислений.

Переменные окружения:
- GPU_ENABLED: включить GPU ускорение (true/false)
- GPU_BACKEND: бэкенд для GPU ('gpu', 'auto', 'cpu')
- GPU_MIN_N: минимальный размер массива для GPU ускорения
"""

import os

# GPU acceleration settings
GPU_ENABLE = os.getenv("GPU_ENABLED", "false").lower() == "true"
GPU_BACKEND = os.getenv("GPU_BACKEND", "cpu")  # 'gpu', 'auto', 'cpu'
GPU_MIN_N = int(os.getenv("GPU_MIN_N", "100"))  # минимальный размер для GPU

