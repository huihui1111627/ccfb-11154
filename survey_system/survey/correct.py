"""改正管理: 自动修正确定性错误 + 审核员人工批注改正(保留改正前后的值)。"""
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .schema import load

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CORRECTIONS_FILE = DATA_DIR / "corrections.jsonl"


def auto_fix(df, quest=None):
    """确定性错误的规则化自动修正:
    - 数值/选项越界 -> 置为缺失(留待插补)
    - 不该答却答了 -> 清空(跳转逻辑是硬约束)
    返回(修正后数据, 修正记录)。
    """
    q = quest or load()
    out = df.copy()
    fixes = []
    for i in out.index:
        row = out.loc[i]
        for qu in q.questions:
            qid, v = qu["id"], row.get(qu["id"])
            if v is None or (isinstance(v, float) and np.isnan(v)):
                continue
            should, unknown = q.condition_holds(qid, row)
            if not unknown and not should:
                fixes.append({"row": int(i), "question": qid, "old": float(v), "new": None,
                              "how": "auto", "why": "不该答却答了,按跳转逻辑清空"})
                out.loc[i, qid] = np.nan
                continue
            bad = False
            if qu["type"] == "choice":
                bad = str(int(v)) not in qu["options"]
            else:
                bad = v < qu["min"] or v > qu["max"]
            if bad:
                fixes.append({"row": int(i), "question": qid, "old": float(v), "new": None,
                              "how": "auto", "why": "取值越界,置为缺失待插补"})
                out.loc[i, qid] = np.nan
    return out, pd.DataFrame(fixes, columns=["row", "question", "old", "new", "how", "why"])


def load_corrections(path=None):
    path = Path(path or CORRECTIONS_FILE)
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def add_correction(row, question, old, new, comment, reviewer="审核员", path=None):
    """人工改正: 追加到 corrections.jsonl, 保留改正前后的值。"""
    rec = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "row": int(row),
           "question": question, "old": old, "new": new,
           "comment": comment, "reviewer": reviewer, "how": "manual"}
    path = Path(path or CORRECTIONS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


def apply_corrections(df, corrections=None):
    """把人工改正应用到数据上(在自动修正之后)。"""
    out = df.copy()
    for c in (corrections if corrections is not None else load_corrections()):
        v = c["new"]
        out.loc[c["row"], c["question"]] = np.nan if v in (None, "", "None") else float(v)
    return out
