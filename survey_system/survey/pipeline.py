"""端到端流程: 造总体 -> 抽样 -> 注入录入错误 -> 审核 -> 修正 -> 插补 -> 加权 -> 汇总 -> 评估。"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import population as pop_mod
from . import correct as corr_mod
from . import estimate as est_mod
from . import impute as imp_mod
from . import validate as val_mod
from . import weight as wgt_mod
from .schema import load

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def run(data_dir=DATA_DIR, verbose=True):
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    log = print if verbose else (lambda *a, **k: None)

    log("1. 生成虚构总体 ...")
    pop = pop_mod.generate_population()
    pop.to_csv(data_dir / "population.csv", index=False)
    log(f"   总体 {len(pop):,} 户; 真值: 季总收入均值 {pop['B0'].mean():.0f}, "
        f"季总支出均值 {pop['C0'].mean():.0f}, 就业率 {(pop['A4'] == 1).mean():.3f}")

    log("2. 分层多阶段抽样 ...")
    clean = pop_mod.draw_sample(pop)
    clean.to_csv(data_dir / "sample_clean.csv", index=False)   # 干净样本(留作评估基准)
    log(f"   样本 {len(clean):,} 户, 设计权数范围 "
        f"[{clean['design_w'].min():.1f}, {clean['design_w'].max():.1f}]")

    log("3. 注入录入错误 ...")
    raw, err_log = pop_mod.inject_errors(clean)
    raw.to_csv(data_dir / "sample_raw.csv", index=False)
    err_log.to_csv(data_dir / "error_injection_log.csv", index=False)
    log(f"   注入错误 {len(err_log)} 处: "
        + ", ".join(f"{k}×{v}" for k, v in err_log["error_type"].value_counts().items()))

    log("4. 审核(范围/跳转/跨题逻辑) ...")
    quest = load()
    issues = val_mod.validate(raw, quest)
    issues.to_csv(data_dir / "issues.csv", index=False)
    log(f"   发现问题 {len(issues)} 条, 涉及问卷 {issues['row'].nunique()} 份")

    log("5. 自动修正确定性错误(越界置缺、不该答却答清空) ...")
    fixed, fixes = corr_mod.auto_fix(raw, quest)
    fixes.to_csv(data_dir / "auto_fixes.csv", index=False)
    log(f"   自动修正 {len(fixes)} 处")

    log("5b. 审核员批注改正(合计按分项重算, 保留前后值) ...")
    sim = simulate_reviewer(fixed)
    manual = corr_mod.load_corrections()          # 网页工作台上人工提交的改正
    all_corr = sim + manual
    if all_corr:
        fixed = corr_mod.apply_corrections(fixed, all_corr)
        pd.DataFrame(all_corr).to_csv(data_dir / "reviewer_fixes.csv", index=False)
    log(f"   模拟批注 {len(sim)} 处 + 人工提交 {len(manual)} 处")
    fixed.to_csv(data_dir / "sample_fixed.csv", index=False)

    log("6. 缺失插补(热卡法 + 回归法) ...")
    imputed, imp_stats = imp_mod.impute(fixed)
    imputed.to_csv(data_dir / "sample_imputed.csv", index=False)
    imp_stats.to_csv(data_dir / "impute_stats.csv", index=False)
    n_imp = int(sum(imputed[c].sum() for c in imputed.columns if c.endswith("_imp")))
    log(f"   插补 {n_imp} 个单元格")

    log("7. 事后加权(设计权数 x 人口控制总数 raking) ...")
    imputed["final_w"] = wgt_mod.post_stratify(imputed, pop)
    imputed.to_csv(data_dir / "sample_final.csv", index=False)
    log(f"   最终权数范围 [{imputed['final_w'].min():.1f}, {imputed['final_w'].max():.1f}]")

    log("8. 加权汇总(含抽样误差与置信区间) ...")
    summary = est_mod.summary_table(imputed)
    summary.to_csv(data_dir / "summary.csv", index=False)

    log("9. 评估: 与总体真值对比 ...")
    report = evaluate(pop, clean, raw, fixed, imputed)
    with open(data_dir / "report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report


def simulate_reviewer(df):
    """模拟审核员: 对合计与分项不符的问卷, 按分项重算合计并留批注(保留前后值)。"""
    recs = []
    comp_inc = ["B2", "B3", "B4"]
    comp_exp = ["C1", "C2", "C3", "C4", "C5"]
    for i in df.index:
        row = df.loc[i]
        if row[comp_inc + ["B0"]].notna().all():
            s = (row["B1"] if pd.notna(row["B1"]) else 0.0) + row[comp_inc].sum()
            if abs(row["B0"] - s) > 1:
                recs.append({"row": int(i), "question": "B0", "old": float(row["B0"]),
                             "new": float(s), "how": "manual",
                             "comment": "合计与分项不符,按分项重算总收入", "reviewer": "模拟审核员"})
        if row[comp_exp + ["C0"]].notna().all():
            s = row[comp_exp].sum()
            if abs(row["C0"] - s) > 1:
                recs.append({"row": int(i), "question": "C0", "old": float(row["C0"]),
                             "new": float(s), "how": "manual",
                             "comment": "合计与分项不符,按分项重算总支出", "reviewer": "模拟审核员"})
    return recs


def _wmean(df, y, w):
    d = df[[y, w]].dropna()
    return float((d[y] * d[w]).sum() / d[w].sum())


def evaluate(pop, clean, raw, fixed, imputed):
    """四份数据对比: 真值 / 脏数据 / 修正后(未插补) / 修正+插补+事后加权。"""
    truth = {"季总收入均值": float(pop["B0"].mean()),
             "季总支出均值": float(pop["C0"].mean()),
             "就业率": float((pop["A4"] == 1).mean())}
    stages = {}
    for name, df, wcol, complete_only in [
            ("脏数据(设计权数)", raw, "design_w", True),
            ("修正后未插补(设计权数)", fixed, "design_w", True),
            ("修正+插补(设计权数)", imputed, "design_w", False),
            ("修正+插补+事后加权", imputed, "final_w", False)]:
        if complete_only:
            inc = _wmean(df, "B0", wcol)
            exp = _wmean(df, "C0", wcol)
            emp = float(df.loc[df["A4"].notna(), wcol][df["A4"].dropna() == 1].sum()
                        / df.loc[df["A4"].notna(), wcol].sum())
        else:
            inc = _wmean(df, "B0", wcol)
            exp = _wmean(df, "C0", wcol)
            emp = float(df.loc[df["A4"] == 1, wcol].sum() / df[wcol].sum())
        stages[name] = {"季总收入均值": round(inc, 1), "季总支出均值": round(exp, 1),
                        "就业率": round(emp, 4),
                        "收入偏差%": round((inc / truth["季总收入均值"] - 1) * 100, 2),
                        "支出偏差%": round((exp / truth["季总支出均值"] - 1) * 100, 2),
                        "就业率偏差pp": round((emp - truth["就业率"]) * 100, 2)}

    # 插补精度: 用干净样本里被抹掉前的真值, 直接算插补值偏差
    imp_eval = {}
    for var in ["B1", "B2", "B0", "C0", "C1"]:
        flag = imputed.get(f"{var}_imp")
        if flag is None or flag.sum() == 0:
            continue
        idx = imputed.index[flag > 0]
        true_v = clean.loc[idx, var]
        imp_v = imputed.loc[idx, var]
        ok = true_v.notna()
        if ok.sum() == 0:
            continue
        bias = float((imp_v[ok] - true_v[ok]).mean())
        rel = float((imp_v[ok].mean() / true_v[ok].mean() - 1) * 100)
        rmse = float(np.sqrt(((imp_v[ok] - true_v[ok]) ** 2).mean()))
        imp_eval[var] = {"插补数": int(ok.sum()), "均值相对偏差%": round(rel, 2),
                         "平均绝对偏差": round(bias, 1), "RMSE": round(rmse, 1)}
    return {"总体真值": {k: round(v, 4) for k, v in truth.items()},
            "各阶段估计": stages, "插补精度": imp_eval}


if __name__ == "__main__":
    rep = run()
    print(json.dumps(rep, ensure_ascii=False, indent=2))
