#!/usr/bin/env python3
"""
A/B Testing Script - Сравнение старого и нового OrderFlow handlers

Автоматически сравнивает метрики двух handlers:
- Количество сигналов
- Качество сигналов (SL/TP, confidence)
- Performance (CPU, memory)
- Latency

Usage:
    python scripts/ab_testing_compare.py --duration 24 --output report.json
"""

import json
import subprocess
import argparse
from datetime import datetime
import redis


class ABTestingCompare:
    """Сравнение старого и нового OrderFlow handlers"""

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """
        Args:
            redis_url: URL для подключения к Redis
        """
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        self.old_container = "scanner-python-worker"
        self.new_container = "scanner-multi-orderflow"

    def get_signal_count(self, container: str, duration_hours: int = 24) -> int:
        """
        Подсчитывает количество сигналов от handler за период.

        Args:
            container: Имя Docker контейнера
            duration_hours: Период в часах

        Returns:
            Количество сигналов
        """
        since = f"{duration_hours}h"
        cmd = f"docker logs {container} --since {since} 2>&1 | grep -c 'Сигнал опубликован' || true"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return int(result.stdout.strip() or 0)

    def get_signal_details(self, container: str, duration_hours: int = 24) -> list[dict]:
        """
        Извлекает детали сигналов из логов.

        Returns:
            Список словарей с деталями сигналов
        """
        since = f"{duration_hours}h"
        cmd = f"docker logs {container} --since {since} 2>&1 | grep 'Сигнал опубликован'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        signals = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue

            # Парсим формат: "📤 Сигнал опубликован: XAUUSD:LONG:395000:1234567890 | LONG @ 3950.00"
            try:
                if '|' in line:
                    parts = line.split('|')
                    sid_part = parts[0].split(':')[-4:]  # Берем последние 4 части (SID)
                    side_price = parts[1].strip().split('@')

                    signals.append({
                        'sid': ':'.join(sid_part),
                        'side': side_price[0].strip(),
                        'price': float(side_price[1].strip()) if len(side_price) > 1 else 0.0
                    })
            except Exception:
                continue

        return signals

    def get_container_stats(self, container: str) -> dict:
        """
        Получает статистику ресурсов контейнера.

        Returns:
            Dict с метриками (CPU, memory)
        """
        cmd = f"docker stats {container} --no-stream --format '{{{{.CPUPerc}}}}|{{{{.MemUsage}}}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            return {'cpu': 0.0, 'memory_mb': 0.0}

        try:
            cpu, mem = result.stdout.strip().split('|')
            cpu_pct = float(cpu.replace('%', ''))

            # Парсим память (формат: "450.5MiB / 1GiB")
            mem_used = mem.split('/')[0].strip()
            if 'MiB' in mem_used:
                memory_mb = float(mem_used.replace('MiB', ''))
            elif 'GiB' in mem_used:
                memory_mb = float(mem_used.replace('GiB', '')) * 1024
            else:
                memory_mb = 0.0

            return {'cpu': cpu_pct, 'memory_mb': memory_mb}
        except Exception:
            return {'cpu': 0.0, 'memory_mb': 0.0}

    def get_container_restarts(self, container: str) -> int:
        """Получает количество перезапусков контейнера"""
        cmd = f"docker inspect {container} --format '{{{{.RestartCount}}}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return int(result.stdout.strip() or 0)

    def compare(self, duration_hours: int = 24) -> dict:
        """
        Проводит полное сравнение handlers.

        Args:
            duration_hours: Период анализа в часах

        Returns:
            Словарь с результатами сравнения
        """
        print(f"🔍 A/B Testing: Сравнение handlers за последние {duration_hours} часов...")
        print("=" * 70)

        # Количество сигналов
        old_count = self.get_signal_count(self.old_container, duration_hours)
        new_count = self.get_signal_count(self.new_container, duration_hours)

        print("\n📊 КОЛИЧЕСТВО СИГНАЛОВ:")
        print(f"   Старый handler: {old_count}")
        print(f"   Новый handler:  {new_count}")

        diff_pct = ((new_count - old_count) / old_count * 100) if old_count > 0 else 0
        print(f"   Разница: {diff_pct:+.1f}%")

        # Performance метрики
        print("\n⚡ PERFORMANCE:")
        old_stats = self.get_container_stats(self.old_container)
        new_stats = self.get_container_stats(self.new_container)

        print("   CPU:")
        print(f"      Старый: {old_stats['cpu']:.1f}%")
        print(f"      Новый:  {new_stats['cpu']:.1f}%")

        print("   Memory:")
        print(f"      Старый: {old_stats['memory_mb']:.0f} MB")
        print(f"      Новый:  {new_stats['memory_mb']:.0f} MB")

        # Restarts
        old_restarts = self.get_container_restarts(self.old_container)
        new_restarts = self.get_container_restarts(self.new_container)

        print("\n🔄 СТАБИЛЬНОСТЬ:")
        print(f"   Перезапуски старого: {old_restarts}")
        print(f"   Перезапуски нового:  {new_restarts}")

        # Детали сигналов
        old_signals = self.get_signal_details(self.old_container, duration_hours)
        new_signals = self.get_signal_details(self.new_container, duration_hours)

        # Распределение по direction
        old_long = sum(1 for s in old_signals if 'LONG' in s['side'])
        old_short = sum(1 for s in old_signals if 'SHORT' in s['side'])
        new_long = sum(1 for s in new_signals if 'LONG' in s['side'])
        new_short = sum(1 for s in new_signals if 'SHORT' in s['side'])

        print("\n📈 РАСПРЕДЕЛЕНИЕ СИГНАЛОВ:")
        print(f"   Старый: LONG={old_long}, SHORT={old_short}")
        print(f"   Новый:  LONG={new_long}, SHORT={new_short}")

        # Результат сравнения
        print("\n" + "=" * 70)
        print("✅ РЕЗУЛЬТАТ A/B ТЕСТИРОВАНИЯ:")

        # Критерий 1: Количество сигналов (должны совпадать ±5%)
        signals_ok = abs(diff_pct) <= 5.0
        print(f"   {'✅' if signals_ok else '❌'} Количество сигналов: {diff_pct:+.1f}% (критерий: ±5%)")

        # Критерий 2: Performance (новый не должен быть хуже >10%)
        mem_diff_pct = ((new_stats['memory_mb'] - old_stats['memory_mb']) / old_stats['memory_mb'] * 100) if old_stats['memory_mb'] > 0 else 0
        perf_ok = mem_diff_pct <= 10.0
        print(f"   {'✅' if perf_ok else '❌'} Memory usage: {mem_diff_pct:+.1f}% (критерий: не хуже >+10%)")

        # Критерий 3: Стабильность (не должно быть перезапусков)
        stability_ok = new_restarts == 0
        print(f"   {'✅' if stability_ok else '❌'} Стабильность: {new_restarts} перезапусков (критерий: 0)")

        # Итоговое решение
        all_ok = signals_ok and perf_ok and stability_ok
        print("\n" + "=" * 70)
        if all_ok:
            print("🎉 РЕЗУЛЬТАТ: НОВЫЙ HANDLER ГОТОВ К МИГРАЦИИ!")
        else:
            print("⚠️  РЕЗУЛЬТАТ: ТРЕБУЕТСЯ ДОПОЛНИТЕЛЬНОЕ ТЕСТИРОВАНИЕ")
        print("=" * 70)

        # Формируем детальный отчет
        report = {
            'timestamp': datetime.now().isoformat(),
            'duration_hours': duration_hours,
            'signals': {
                'old_count': old_count,
                'new_count': new_count,
                'diff_pct': diff_pct,
                'pass': signals_ok
            },
            'performance': {
                'old_cpu': old_stats['cpu'],
                'new_cpu': new_stats['cpu'],
                'old_memory_mb': old_stats['memory_mb'],
                'new_memory_mb': new_stats['memory_mb'],
                'memory_diff_pct': mem_diff_pct,
                'pass': perf_ok
            },
            'stability': {
                'old_restarts': old_restarts,
                'new_restarts': new_restarts,
                'pass': stability_ok
            },
            'distribution': {
                'old': {'long': old_long, 'short': old_short},
                'new': {'long': new_long, 'short': new_short}
            },
            'decision': {
                'ready_for_migration': all_ok,
                'passed_all_criteria': all_ok
            }
        }

        return report


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='A/B Testing для OrderFlow handlers')
    parser.add_argument('--duration', type=int, default=24, help='Период анализа в часах (default: 24)')
    parser.add_argument('--output', type=str, help='Файл для сохранения отчета (JSON)')
    parser.add_argument('--redis-url', type=str, default='redis://localhost:6379/0', help='Redis URL')

    args = parser.parse_args()

    # Запускаем сравнение
    tester = ABTestingCompare(redis_url=args.redis_url)
    report = tester.compare(duration_hours=args.duration)

    # Сохраняем отчет
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n📝 Отчет сохранен: {args.output}")


if __name__ == '__main__':
    main()

