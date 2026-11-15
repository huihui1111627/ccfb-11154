"""审核规则引擎: 取值范围、跳转逻辑、跨题逻辑三类校验。"""
import numpy as np
import pandas as pd

from .schema import load


def validate(df, quest=None):
    """对数据框逐行审核, 返回长表: row, hh_id, rule, question, severity, msg。"""
    q = quest or load()
    issues = []

    def add(i, rule, qid, sev, msg):
        issues.append({"row": i, "hh_id": df.loc[i, "hh_id"] if "hh_id" in df.columns else i,
                       "rule": rule, "question": qid, "severity": sev, "msg": msg})

    for i in df.index:
        row = df.loc[i]
        # 1) 必答题缺失 + 取值范围
        for qu in q.questions:
            qid = qu["id"]
            v = row.get(qid)
            missing = v is None or (isinstance(v, float) and np.isnan(v))
            should, unknown = q.condition_holds(qid, row)
            if unknown:
                add(i, "SKIP0", qid, "error",
                    f"前置题缺失, 无法判断{qid}({qu['label']})是否应作答")
                continue
            if should and missing:
                kind = "必答缺失" if not qu.get("condition") else "该答未答(跳转)"
                add(i, "SKIP1", qid, "error", f"{kind}: {qid}({qu['label']})")
                continue
            if not should and not missing:
                add(i, "SKIP2", qid, "error",
                    f"不该答却答了: {qid}({qu['label']}), 前置条件不满足")
                continue
            if missing:
                continue
            # 范围检查
            if qu["type"] == "choice":
                if str(int(v)) not in qu["options"]:
                    add(i, "RANGE", qid, "error",
                        f"选项越界: {qid}={v}, 允许值{list(qu['options'])}")
            else:
                if v < qu["min"] or v > qu["max"]:
                    add(i, "RANGE", qid, "error",
                        f"数值越界: {qid}={v}, 允许[{qu['min']},{qu['max']}]")
        # 2) 跨题逻辑
        for r in q.cross_rules:
            when, unk1 = q.eval_expr(r["when"], row)
            if unk1 or not when:
                continue
            ok, unk2 = q.eval_expr(r["check"], row)
            if unk2:
                add(i, r["id"], "", r["severity"], f"{r['msg']}(相关题缺失无法核实)")
            elif not ok:
                add(i, r["id"], "", r["severity"], r["msg"])

    return pd.DataFrame(issues, columns=["row", "hh_id", "rule", "question", "severity", "msg"])


RULE_NAMES = {
    "SKIP0": "前置缺失无法判断跳转", "SKIP1": "该答未答", "SKIP2": "不该答却答了",
    "RANGE": "取值越界", "R1": "年龄-就业矛盾", "R2": "就业无工资", "R3": "老人无转移收入",
    "R4": "收入合计不符", "R5": "支出合计不符", "R6": "支出远超收入", "R7": "低龄户主复核",
}
