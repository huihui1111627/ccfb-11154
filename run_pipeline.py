#!/usr/bin/env python3
"""端到端跑一遍: 造总体 -> 抽样 -> 造错 -> 审核 -> 修正 -> 插补 -> 加权 -> 汇总 -> 评估。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "survey_system"))

from survey.pipeline import run

if __name__ == "__main__":
    report = run()
    print("\n===== 评估报告 =====")
    print(json.dumps(report, ensure_ascii=False, indent=2))
