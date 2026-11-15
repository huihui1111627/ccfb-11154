"""问卷结构:加载配置、求值跳转条件与跨题校验表达式。"""
import json
import math
import re
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "questionnaire.json"

_VAR_RE = re.compile(r"\b[A-D]\d+z?\b")


class Questionnaire:
    def __init__(self, path=None):
        with open(path or CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
        self.title = cfg["title"]
        self.questions = cfg["questions"]
        self.cross_rules = cfg["cross_rules"]
        self.by_id = {q["id"]: q for q in self.questions}
        self.name2id = {q["name"]: q["id"] for q in self.questions}

    def conditional_questions(self):
        return [q for q in self.questions if q.get("condition")]

    @staticmethod
    def _env(row):
        """构造表达式求值环境:题号 -> 值;条件数值题额外提供补0版本(B1z)。"""
        env = {}
        for k, v in row.items():
            if isinstance(k, str) and _VAR_RE.fullmatch(k):
                env[k] = v
        for k, v in list(env.items()):
            if isinstance(v, float) and math.isnan(v):
                env[k + "z"] = 0.0
            else:
                env[k + "z"] = v
        return env

    @classmethod
    def eval_expr(cls, expr, row):
        """求值表达式。返回 (结果或None, 是否因前置缺失无法判断)。"""
        expr = re.sub(r"\btrue\b", "True", expr)
        expr = re.sub(r"\bfalse\b", "False", expr)
        env = cls._env(row)
        for var in _VAR_RE.findall(expr):
            base = var.rstrip("z")
            v = env.get(base)
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None, True
        try:
            return bool(eval(expr, {"__builtins__": {}, "abs": abs}, env)), False
        except Exception:
            return None, True

    def condition_holds(self, qid, row):
        """判断某题按跳转逻辑是否应当作答。无条件题恒为 True。"""
        cond = self.by_id[qid].get("condition")
        if not cond:
            return True, False
        return self.eval_expr(cond, row)


def load(path=None):
    return Questionnaire(path)
