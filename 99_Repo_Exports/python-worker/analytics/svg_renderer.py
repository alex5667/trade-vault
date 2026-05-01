from __future__ import annotations
"""
SVG Renderer - Генерация ROC и Confusion Matrix в SVG без PIL/matplotlib.

Функции:
- roc_svg() - ROC кривая в SVG формате
- confusion_svg() - Confusion Matrix в SVG
- save_svg() - Сохранение SVG в файл

Преимущества:
- Нет зависимостей (PIL, matplotlib)
- Лёгкие файлы (<50KB)
- Векторная графика (масштабируемость)
- Встраивание в HTML
"""

import os
from typing import List, Dict

from common.log import setup_logger


logger = setup_logger("SVGRenderer")


def _svg_header(width: int, height: int) -> str:
    """Генерация SVG заголовка"""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">\n'
    )


def _axis(width: int, height: int, pad: int = 40) -> str:
    """
    Генерация осей для ROC кривой.
    
    Args:
        width: Ширина SVG
        height: Высота SVG
        pad: Отступ от краёв
        
    Returns:
        SVG разметка осей
    """
    lines = []

    # Рамка
    lines.append(
        f'<rect x="{pad}" y="{pad}" width="{width-2*pad}" '
        f'height="{height-2*pad}" fill="none" stroke="#888" stroke-width="1"/>'
    )

    # Диагональ для ROC (random classifier)
    lines.append(
        f'<line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{pad}" '
        f'stroke="#bbb" stroke-dasharray="4,3"/>'
    )

    # Деления и метки
    for i in range(11):
        x = pad + (width - 2*pad) * i / 10.0
        y = pad + (height - 2*pad) * i / 10.0

        # Вертикальные риски
        lines.append(
            f'<line x1="{x}" y1="{height-pad}" x2="{x}" y2="{height-pad+5}" '
            f'stroke="#999" stroke-width="1"/>'
        )

        # Горизонтальные риски
        lines.append(
            f'<line x1="{pad-5}" y1="{height-y}" x2="{pad}" y2="{height-y}" '
            f'stroke="#999" stroke-width="1"/>'
        )

        # Подписи
        val = f"{i/10:.1f}"
        lines.append(
            f'<text x="{x}" y="{height-pad+18}" font-size="10" '
            f'text-anchor="middle" fill="#666">{val}</text>'
        )
        lines.append(
            f'<text x="{pad-10}" y="{height-y+3}" font-size="10" '
            f'text-anchor="end" fill="#666">{val}</text>'
        )

    # Названия осей
    lines.append(
        f'<text x="{width/2}" y="{height-5}" font-size="12" '
        f'text-anchor="middle" fill="#333">FPR</text>'
    )
    lines.append(
        f'<text x="15" y="{height/2}" font-size="12" text-anchor="middle" '
        f'transform="rotate(-90 15,{height/2})" fill="#333">TPR</text>'
    )

    return "\n".join(lines)


def roc_svg(
    points: List[Dict[str, float]],
    auc: float,
    width: int = 640,
    height: int = 420,
    pad: int = 40,
    color: str = "#2a7"
) -> str:
    """
    Генерация ROC кривой в SVG формате.
    
    Args:
        points: Список точек [{"fpr": ..., "tpr": ...}, ...]
        auc: Area Under Curve
        width: Ширина SVG
        height: Высота SVG
        pad: Отступ от краёв
        color: Цвет линии
        
    Returns:
        SVG разметка ROC кривой
    """
    # Сортируем по FPR
    pts = sorted(points, key=lambda p: p.get("fpr", 0.0))

    if not pts:
        pts = [{"fpr": 0.0, "tpr": 0.0}, {"fpr": 1.0, "tpr": 1.0}]

    # Функции маппинга координат
    def mapx(fpr):
        return pad + (width - 2*pad) * float(max(0, min(1, fpr)))

    def mapy(tpr):
        return height - pad - (height - 2*pad) * float(max(0, min(1, tpr)))

    # Построение path для линии
    d = []
    for i, p in enumerate(pts):
        x = mapx(p.get("fpr", 0.0))
        y = mapy(p.get("tpr", 0.0))

        if i == 0:
            d.append(f"M{x},{y}")
        else:
            d.append(f"L{x},{y}")

    path = " ".join(d)

    # Сборка SVG
    svg = [_svg_header(width, height)]
    svg.append(_axis(width, height, pad))

    # Линия ROC
    svg.append(
        f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.2"/>'
    )

    # Точки
    for p in pts:
        x = mapx(p.get("fpr", 0.0))
        y = mapy(p.get("tpr", 0.0))
        svg.append(f'<circle cx="{x}" cy="{y}" r="2.5" fill="{color}"/>')

    # Заголовок
    svg.append(
        f'<text x="{width/2}" y="22" font-size="14" text-anchor="middle" '
        f'fill="#222">ROC Curve (AUC={auc:.3f})</text>'
    )

    svg.append("</svg>")

    return "\n".join(svg)


def confusion_svg(
    tp: int,
    fp: int,
    tn: int,
    fn: int,
    width: int = 420,
    height: int = 320
) -> str:
    """
    Генерация Confusion Matrix в SVG формате.
    
    Args:
        tp: True Positives
        fp: False Positives
        tn: True Negatives
        fn: False Negatives
        width: Ширина SVG
        height: Высота SVG
        
    Returns:
        SVG разметка Confusion Matrix
    """
    pad = 20
    cellw = (width - 2*pad) / 2
    cellh = (height - 2*pad) / 2

    svg = [_svg_header(width, height)]

    # Заголовок
    svg.append(
        f'<text x="{width/2}" y="18" font-size="14" text-anchor="middle" '
        f'fill="#222">Confusion Matrix</text>'
    )

    # Рамки ячеек
    for r in range(2):
        for c in range(2):
            x = pad + c * cellw
            y = pad + r * cellh + 10
            svg.append(
                f'<rect x="{x}" y="{y}" width="{cellw}" height="{cellh}" '
                f'fill="none" stroke="#888"/>'
            )

    # Подписи столбцов и строк
    svg.append(
        f'<text x="{pad+cellw/2}" y="{pad+8}" font-size="12" '
        f'text-anchor="middle" fill="#333">Pred: Positive</text>'
    )
    svg.append(
        f'<text x="{pad+1.5*cellw}" y="{pad+8}" font-size="12" '
        f'text-anchor="middle" fill="#333">Pred: Negative</text>'
    )
    svg.append(
        f'<text x="{pad-6}" y="{pad+cellh/2+18}" font-size="12" '
        f'text-anchor="end" fill="#333">Real: Positive</text>'
    )
    svg.append(
        f'<text x="{pad-6}" y="{pad+1.5*cellh+18}" font-size="12" '
        f'text-anchor="end" fill="#333">Real: Negative</text>'
    )

    # Значения в ячейках
    def cell(xc, yc, val, color):
        return (
            f'<text x="{xc}" y="{yc}" font-size="18" text-anchor="middle" '
            f'fill="{color}">{val}</text>'
        )

    svg.append(cell(pad + cellw/2, pad + cellh/2 + 18, tp, "#2a7"))      # TP
    svg.append(cell(pad + 1.5*cellw, pad + cellh/2 + 18, fn, "#c33"))    # FN
    svg.append(cell(pad + cellw/2, pad + 1.5*cellh + 18, fp, "#c33"))    # FP
    svg.append(cell(pad + 1.5*cellw, pad + 1.5*cellh + 18, tn, "#2a7"))  # TN

    svg.append("</svg>")

    return "\n".join(svg)


def save_svg(path: str, svg_text: str):
    """
    Сохранение SVG в файл.
    
    Args:
        path: Путь к файлу
        svg_text: SVG разметка
    """
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            f.write(svg_text)

        logger.info(f"✅ SVG сохранён: {path}")

    except Exception as e:
        logger.error(f"❌ Ошибка сохранения SVG: {e}")

