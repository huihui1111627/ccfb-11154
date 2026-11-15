"""加权汇总估计: 总量/均值/比例 + 抽样误差(按分层多阶段设计的最终类方差) + 置信区间。

方差按"最终类(ultimate cluster)"近似: 以PSU为最终抽样单位,
Var(t) = sum_h m_h/(m_h-1) * sum_i (z_hi - zbar_h)^2,  z_hi = sum_j w_hij * y_hij
均值/比例用比率估计的线性化: g = (y - r*x)/N_hat
"""
import numpy as np
import pandas as pd

MIN_CELL_N = 30      # 单元格最小样本量
MAX_CV = 0.30        # 最大可接受变异系数


def _cluster_scores(df, g, w):
    """每个PSU的加权得分 z_hi = sum(w*g)。"""
    tmp = pd.DataFrame({"stratum": df["stratum"], "psu": df["psu"], "z": g * w})
    return tmp.groupby(["stratum", "psu"])["z"].sum().reset_index()


def _var_of_total(df, g, w):
    z = _cluster_scores(df, g, w)
    var = 0.0
    for stratum, s in z.groupby("stratum"):
        m = len(s)
        if m < 2:
            continue
        dev = s["z"] - s["z"].mean()
        var += m / (m - 1) * (dev ** 2).sum()
    return var


def estimate(df, y, weight="final_w", domain=None):
    """加权估计 y 的总量与人均值。
    domain: 可选布尔Series, 估计该子总体(域)。
    返回 dict(总量, 总量SE, 均值, 均值SE, CV, CI下限, CI上限, 样本量, 可靠)。"""
    d = df if domain is None else df[domain]
    w = d[weight].to_numpy(dtype=float)
    yv = d[y].to_numpy(dtype=float)
    ind = np.ones(len(d))
    n = len(d)
    if n == 0:
        return {"总量": 0, "均值": np.nan, "样本量": 0, "可靠": False, "备注": "无样本"}
    N_hat = w.sum()
    total = float((w * yv).sum())
    mean = total / N_hat if N_hat else np.nan
    var_t = _var_of_total(d, yv, w)
    se_t = float(np.sqrt(var_t))
    # 均值的方差: 线性化 g = y - mean
    var_m = _var_of_total(d, yv - mean, w) / N_hat ** 2
    se_m = float(np.sqrt(var_m))
    cv = se_m / abs(mean) if mean else np.inf
    reliable = (n >= MIN_CELL_N) and (cv <= MAX_CV)
    note = "" if reliable else ("样本量过小" if n < MIN_CELL_N else "变异系数过大")
    return {"总量": round(total, 0), "总量SE": round(se_t, 0),
            "均值": round(mean, 1), "均值SE": round(se_m, 1),
            "CI下限": round(mean - 1.96 * se_m, 1), "CI上限": round(mean + 1.96 * se_m, 1),
            "CV": round(cv, 3), "样本量": n, "可靠": reliable, "备注": note}


def estimate_proportion(df, cond, weight="final_w", domain=None):
    """比例估计(如就业率): cond 为布尔Series。"""
    d = df if domain is None else df[domain]
    w = d[weight].to_numpy(dtype=float)
    x = cond.loc[d.index].to_numpy(dtype=float)
    n = len(d)
    N_hat = w.sum()
    p = float((w * x).sum() / N_hat)
    var = _var_of_total(d, x - p, w) / N_hat ** 2
    se = float(np.sqrt(var))
    cv = se / p if p else np.inf
    reliable = (n >= MIN_CELL_N) and (cv <= MAX_CV)
    return {"比例": round(p, 4), "SE": round(se, 4),
            "CI下限": round(max(0, p - 1.96 * se), 4), "CI上限": round(min(1, p + 1.96 * se), 4),
            "CV": round(cv, 3), "样本量": n, "可靠": reliable,
            "备注": "" if reliable else ("样本量过小" if n < MIN_CELL_N else "变异系数过大")}


def summary_table(df, weight="final_w"):
    """主要指标汇总表: 总体 + 分城乡 + 分年龄段。"""
    rows = []
    age_grp = pd.cut(df["A2"], [0, 39, 59, 200], labels=["16-39岁", "40-59岁", "60岁及以上"])
    domains = [("全市", None), ("城镇", df["urban"] == 1), ("农村", df["urban"] == 0)]
    for g in ["16-39岁", "40-59岁", "60岁及以上"]:
        domains.append((g, (age_grp == g).to_numpy()))
    for name, dom in domains:
        for var, label in [("B0", "季总收入"), ("C0", "季总支出"), ("B1", "工资性收入")]:
            r = estimate(df, var, weight, dom)
            r.update({"分组": name, "指标": label})
            rows.append(r)
        r = estimate_proportion(df, df["A4"] == 1, weight, dom)
        r.update({"分组": name, "指标": "就业率"})
        rows.append(r)
    cols = ["分组", "指标", "均值", "均值SE", "CI下限", "CI上限", "CV",
            "比例", "SE", "总量", "总量SE", "样本量", "可靠", "备注"]
    df_out = pd.DataFrame(rows)
    return df_out[[c for c in cols if c in df_out.columns]]
