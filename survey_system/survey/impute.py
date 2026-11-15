"""缺失插补: 热卡法(分层层内随机 donor) + 回归法(线性模型预测), 插补值打标记。

- B1 工资性收入、C1 食品支出: 回归法(log线性模型 + 残差噪声)
- 其余数值题: 热卡法, 插补类 = 城乡 x 年龄段 x 性别, 逐级回退
- 插补标记: 每题生成 <题号>_imp 列 (0=未插补, 1=热卡, 2=回归)
"""
import numpy as np
import pandas as pd

HOTDECK_VARS = ["A1", "A3", "A5", "B2", "B3", "B4", "B0", "D1", "D2",
                "C2", "C3", "C4", "C5", "C0"]
REGRESS_VARS = ["B1", "C1"]


def _age_group(age):
    return np.where(age < 40, 0, np.where(age < 60, 1, 2))


def _imputation_classes(df):
    return list(zip(df["urban"].fillna(-1).astype(int),
                    _age_group(df["A2"].fillna(45).to_numpy()),
                    df["A1"].fillna(1).astype(int)))


def hot_deck(df, var, rng):
    out = df[var].copy()
    classes = _imputation_classes(df)
    miss = out.isna()
    if not miss.any():
        return out, pd.Series(0, index=df.index)
    flag = pd.Series(0, index=df.index)
    # 逐级回退的类定义: (城乡,年龄组,性别) -> (年龄组,性别) -> (年龄组) -> 全体
    full = df.copy()
    keys_full = classes
    keys_mid = list(zip(_age_group(full["A2"].fillna(45).to_numpy()),
                        full["A1"].fillna(1).astype(int)))
    keys_lo = list(_age_group(full["A2"].fillna(45).to_numpy()))
    donors_full = pd.Series(out.values).groupby(pd.Index(keys_full))
    donors_mid = pd.Series(out.values).groupby(pd.Index(keys_mid))
    donors_lo = pd.Series(out.values).groupby(pd.Index(keys_lo))
    pool_all = out.dropna().to_numpy()

    def draw(pool):
        return rng.choice(pool) if len(pool) else None

    for i in df.index[miss]:
        v = None
        for table, key in ((donors_full, keys_full[i]), (donors_mid, keys_mid[i]),
                           (donors_lo, keys_lo[i])):
            if key in table.groups:
                pool = table.get_group(key).dropna().to_numpy()
                v = draw(pool)
                if v is not None:
                    break
        if v is None:
            v = draw(pool_all)
        out.loc[i] = v
        flag.loc[i] = 1
    return out, flag


def _design_matrix(df):
    age = df["A2"].fillna(df["A2"].median()).to_numpy()
    edu = df["A3"].fillna(df["A3"].median()).to_numpy()
    urban = df["urban"].fillna(1).to_numpy()
    gender = df["A1"].fillna(1).to_numpy()
    return np.column_stack([np.ones(len(df)), age, age ** 2 / 1000, edu, urban, gender])


def regression_impute(df, var, rng):
    """log(y+1) ~ 年龄 + 年龄^2 + 文化 + 城乡 + 性别, 预测后加残差重抽样噪声。"""
    out = df[var].copy()
    miss = out.isna()
    flag = pd.Series(0, index=df.index)
    if not miss.any():
        return out, flag
    obs = ~miss
    X = _design_matrix(df)
    y = np.log1p(out[obs].to_numpy(dtype=float))
    beta, *_ = np.linalg.lstsq(X[obs.to_numpy()], y, rcond=None)
    resid = y - X[obs.to_numpy()] @ beta
    noise = rng.choice(resid, size=int(miss.sum()), replace=True)
    pred = np.expm1(X[miss.to_numpy()] @ beta + noise)
    out.loc[miss] = np.clip(pred, 0, None)
    flag.loc[miss] = 2
    return out, flag


def impute(df, seed=20240501):
    """对整表插补。返回(插补后数据含 *_imp 标记列, 插补统计)。"""
    rng = np.random.default_rng(seed)
    out = df.reset_index(drop=True).copy()
    stats = []
    # A2年龄缺失会影响插补类与回归, 先用热卡补A2
    if out["A2"].isna().any():
        med = out.groupby(["urban"])["A2"].transform("median")
        out["A2"] = out["A2"].fillna(med).fillna(out["A2"].median())
        out["A2_imp"] = (df["A2"].isna()).astype(int)
    else:
        out["A2_imp"] = 0
    # A4就业状态缺失: 热卡(影响B1跳转)
    if out["A4"].isna().any():
        vals, fl = hot_deck(out, "A4", rng)
        out["A4"] = vals
        out["A4_imp"] = fl
    else:
        out["A4_imp"] = 0
    for var in REGRESS_VARS + HOTDECK_VARS:
        if var not in out.columns:
            continue
        n_miss = int(out[var].isna().sum())
        if var in REGRESS_VARS:
            out[var], flag = regression_impute(out, var, rng)
            method = "回归法"
        else:
            out[var], flag = hot_deck(out, var, rng)
            method = "热卡法"
        out[f"{var}_imp"] = flag.astype(int)
        stats.append({"question": var, "缺失数": n_miss, "插补方法": method,
                      "插补后均值": round(float(out[var].mean()), 1)})
    return out, pd.DataFrame(stats)
