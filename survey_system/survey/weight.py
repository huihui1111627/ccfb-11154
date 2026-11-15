"""加权: 设计权数 + 事后加权(按人口控制总数做迭代比例拟合 raking)。"""
import numpy as np
import pandas as pd


def rake(df, weight_col, margins, max_iter=200, tol=1e-6):
    """迭代比例拟合(IPF)。margins: [(变量名, {水平: 总体控制数}), ...]
    返回校准后的权数 Series。"""
    w = df[weight_col].astype(float).copy().to_numpy()
    for _ in range(max_iter):
        max_dev = 0.0
        for var, targets in margins:
            col = df[var]
            cur = pd.Series(w).groupby(col.values).sum()
            for level, target in targets.items():
                got = cur.get(level, 0.0)
                if got > 0:
                    ratio = target / got
                    mask = np.array([x == level for x in col])
                    w = np.where(mask, w * ratio, w)
                    max_dev = max(max_dev, abs(ratio - 1.0))
        if max_dev < tol:
            break
    return pd.Series(w, index=df.index, name="final_w")


def control_totals_from_population(pop):
    """从总体构造控制总数: 性别x年龄段 边际 + 城乡边际。"""
    age_grp = pd.cut(pop["A2"], [0, 39, 59, 200], labels=[0, 1, 2])
    tmp = pop.assign(age_grp=age_grp)
    m1 = tmp.groupby(["A1", "age_grp"], observed=True).size().to_dict()
    margin1 = {(k[0], k[1]): v for k, v in m1.items()}
    m2 = pop.groupby("urban").size().to_dict()
    return margin1, m2


def post_stratify(df, pop, weight_col="design_w"):
    """事后加权: 第一维 性别x年龄段, 第二维 城乡。"""
    m1, m2 = control_totals_from_population(pop)
    age_grp = pd.cut(df["A2"], [0, 39, 59, 200], labels=[0, 1, 2])
    d = df.assign(age_grp=age_grp, sex_age=list(zip(df["A1"], age_grp)))
    w = rake(d, weight_col, [("sex_age", m1), ("urban", m2)])
    return w
