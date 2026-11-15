"""调查数据处理工作台 (Flask)。

板块: 问卷结构 / 审核规则 / 疑问问卷 / 缺失插补 / 加权汇总
启动: python3 app.py  (先运行 run_pipeline.py 生成数据)
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from flask import Flask, abort, redirect, render_template, request, url_for

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

from survey import correct as corr_mod
from survey import estimate as est_mod
from survey import impute as imp_mod
from survey import validate as val_mod
from survey import weight as wgt_mod
from survey.schema import load as load_quest

DATA = BASE / "data"
app = Flask(__name__)
quest = load_quest()
_cache = {}


def data(name):
    return pd.read_csv(DATA / name)


def get(key, loader):
    if key not in _cache:
        _cache[key] = loader()
    return _cache[key]


def recompute():
    """从原始录入数据重跑: 自动修正 -> 人工改正 -> 插补 -> 加权 -> 汇总。"""
    raw = data("sample_raw.csv")
    fixed, _ = corr_mod.auto_fix(raw, quest)
    manual = corr_mod.load_corrections()
    if manual:
        fixed = corr_mod.apply_corrections(fixed, manual)
    imputed, imp_stats = imp_mod.impute(fixed)
    pop = get("population", lambda: data("population.csv"))
    imputed["final_w"] = wgt_mod.post_stratify(imputed, pop)
    summary = est_mod.summary_table(imputed)
    imputed.to_csv(DATA / "sample_final.csv", index=False)
    summary.to_csv(DATA / "summary.csv", index=False)
    imp_stats.to_csv(DATA / "impute_stats.csv", index=False)
    _cache.clear()
    return len(manual)


@app.route("/")
def dashboard():
    issues = get("issues", lambda: data("issues.csv"))
    final = get("final", lambda: data("sample_final.csv"))
    stats = {
        "样本量": len(final),
        "问题条数": len(issues),
        "疑问问卷数": issues["row"].nunique(),
        "人工改正数": len(corr_mod.load_corrections()),
        "插补单元格数": int(sum(final[c].sum() for c in final.columns if c.endswith("_imp"))),
        "权数范围": f"{final['final_w'].min():.1f} ~ {final['final_w'].max():.1f}",
    }
    return render_template("dashboard.html", stats=stats, title=quest.title)


@app.route("/structure")
def structure():
    return render_template("structure.html", q=quest, title=quest.title)


@app.route("/rules")
def rules():
    issues = get("issues", lambda: data("issues.csv"))
    counts = (issues.groupby(["rule", "severity"]).size().reset_index(name="条数"))
    counts["规则"] = counts["rule"].map(val_mod.RULE_NAMES).fillna(counts["rule"])
    cross = [{"id": r["id"], "msg": r["msg"], "severity": r["severity"],
              "when": r["when"], "check": r["check"]} for r in quest.cross_rules]
    return render_template("rules.html", counts=counts.to_dict("records"),
                           cross=cross, title=quest.title)


@app.route("/queries")
def queries():
    issues = get("issues", lambda: data("issues.csv"))
    rule_filter = request.args.get("rule")
    sev = request.args.get("severity")
    df = issues.copy()
    if rule_filter:
        df = df[df["rule"] == rule_filter]
    if sev:
        df = df[df["severity"] == sev]
    grouped = (df.groupby(["row", "hh_id"]).size().reset_index(name="问题数")
                 .sort_values("问题数", ascending=False).head(300))
    return render_template("queries.html", rows=grouped.to_dict("records"),
                           rules=sorted(issues["rule"].unique()),
                           rule_names=val_mod.RULE_NAMES,
                           cur_rule=rule_filter, cur_sev=sev, title=quest.title)


@app.route("/queries/<int:row>", methods=["GET", "POST"])
def query_detail(row):
    raw = get("raw", lambda: data("sample_raw.csv"))
    if row < 0 or row >= len(raw):
        abort(404)
    if request.method == "POST":
        qid = request.form["question"]
        new_v = request.form["new_value"].strip()
        comment = request.form["comment"].strip()
        old_v = raw.loc[row, qid]
        old_v = None if pd.isna(old_v) else float(old_v)
        corr_mod.add_correction(row, qid, old_v, new_v if new_v else None,
                                comment, reviewer=request.form.get("reviewer", "审核员"))
        n = recompute()
        return redirect(url_for("query_detail", row=row, saved=1, applied=n))
    issues = get("issues", lambda: data("issues.csv"))
    rec_issues = issues[issues["row"] == row]
    record = raw.loc[row]
    fields = []
    for qu in quest.questions:
        qid = qu["id"]
        v = record.get(qid)
        fields.append({"id": qid, "label": qu["label"], "type": qu["type"],
                       "value": "" if pd.isna(v) else (int(v) if float(v).is_integer() else round(v, 1)),
                       "options": qu.get("options"),
                       "condition": qu.get("condition", "")})
    corrections = [c for c in corr_mod.load_corrections() if c["row"] == row]
    return render_template("query_detail.html", row=row, hh_id=record["hh_id"],
                           issues=rec_issues.to_dict("records"), fields=fields,
                           corrections=corrections,
                           saved=request.args.get("saved"), title=quest.title)


@app.route("/imputation")
def imputation():
    stats = get("imp_stats", lambda: data("impute_stats.csv"))
    final = get("final", lambda: data("sample_final.csv"))
    clean = get("clean", lambda: data("sample_clean.csv"))
    # 插补前后偏差: 用干净样本真值对比
    rows = []
    for var in ["B1", "B2", "B0", "C0", "C1"]:
        flag = final.get(f"{var}_imp")
        if flag is None or flag.sum() == 0:
            continue
        idx = final.index[flag > 0]
        true_v, imp_v = clean.loc[idx, var], final.loc[idx, var]
        ok = true_v.notna()
        rows.append({"question": var, "插补数": int(ok.sum()),
                     "真值均值": round(float(true_v[ok].mean()), 1),
                     "插补均值": round(float(imp_v[ok].mean()), 1),
                     "相对偏差%": round(float((imp_v[ok].mean() / true_v[ok].mean() - 1) * 100), 2)})
    return render_template("imputation.html", stats=stats.to_dict("records"),
                           bias=rows, title=quest.title)


@app.route("/summary")
def summary():
    tbl = get("summary", lambda: data("summary.csv"))
    pop = get("population", lambda: data("population.csv"))
    truth = {"季总收入": round(float(pop["B0"].mean()), 1),
             "季总支出": round(float(pop["C0"].mean()), 1),
             "就业率": round(float((pop["A4"] == 1).mean()), 4)}
    return render_template("summary.html", rows=tbl.to_dict("records"),
                           truth=truth, title=quest.title)


@app.route("/recompute", methods=["POST"])
def do_recompute():
    n = recompute()
    return redirect(url_for("summary", applied=n))


if __name__ == "__main__":
    app.run(debug=False, port=8501)
