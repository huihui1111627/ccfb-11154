"""虚构总体生成 + 分层多阶段抽样 + 录入错误注入。

总体: 10个层(5个区县 x 城乡), 每层25个社区(PSU), 每社区400户, 共10万户。
抽样: 每层按PPS抽8个社区, 社区内等距抽50户, 共4000户。
各层抽样比不同(城镇超配), 设计权数因而有差异。
"""
import numpy as np
import pandas as pd

N_DISTRICTS = 5
PSUS_PER_STRATUM = 25
HH_PER_PSU = 400
M_PSU = 8          # 每层抽中的社区数
N_HH = 50          # 每个抽中社区调查的户数


def generate_population(seed=20240101):
    rng = np.random.default_rng(seed)
    rows = []
    for d in range(1, N_DISTRICTS + 1):
        for urban in (1, 0):
            stratum = f"D{d}_{'U' if urban else 'R'}"
            # 区县富裕程度、城镇化水平差异
            district_gdp = rng.uniform(0.8, 1.3)
            for p in range(1, PSUS_PER_STRATUM + 1):
                psu = f"{stratum}_P{p:02d}"
                psu_effect = rng.normal(0, 0.15)          # 社区聚类效应(组内相关)
                n = HH_PER_PSU
                age = np.clip(rng.normal(47, 14, n), 16, 92).round().astype(int)
                gender = rng.binomial(1, 0.52, n) + 1      # 1男 2女
                # 文化程度与年龄、城乡相关
                edu_base = np.where(urban == 1, 2.9, 2.1) - (age - 40) / 40
                education = np.clip(np.round(edu_base + rng.normal(0, 0.9, n)), 1, 5).astype(int)
                hh_size = np.clip(np.round(rng.normal(3.0 if urban else 3.6, 1.1, n)), 1, 10).astype(int)
                # 就业状态: 16-60岁参与劳动力, 参与率随年龄下降
                in_lf = (age < 60) & (rng.random(n) < np.clip(0.95 - 0.012 * np.maximum(age - 45, 0), 0.3, 0.95))
                unemployed = in_lf & (rng.random(n) < (0.05 if urban else 0.03))
                emp_status = np.where(unemployed, 2, np.where(in_lf, 1, 3))
                # 收入(季度, 元)
                skill = 0.25 * education + 0.15 * urban + district_gdp
                wage = np.where(
                    emp_status == 1,
                    np.exp(rng.normal(8.6 + 0.55 * skill + psu_effect, 0.45, n)), 0.0)
                has_biz = rng.random(n) < (0.10 if urban else 0.16)
                business = np.where(has_biz, np.exp(rng.normal(7.8 + 0.3 * skill + psu_effect, 0.7, n)), 0.0)
                property_inc = np.where(rng.random(n) < 0.35,
                                        np.exp(rng.normal(6.5 + 0.4 * urban + psu_effect, 0.8, n)), 0.0)
                transfer = np.where(age >= 60,
                                    np.exp(rng.normal(7.6 + 0.5 * urban, 0.4, n)),
                                    np.where(rng.random(n) < 0.08, rng.uniform(500, 3000, n), 0.0))
                total_income = wage + business + property_inc + transfer
                # 支出: 随收入递增、恩格尔系数递减
                engel = np.clip(0.45 - 0.06 * np.log1p(total_income) / 3 + rng.normal(0, 0.04, n), 0.15, 0.6)
                cons_prop = np.clip(rng.normal(0.72, 0.12, n), 0.3, 1.2)
                total_exp = total_income * cons_prop + rng.normal(0, 800, n)
                total_exp = np.clip(total_exp, 1500, None)
                food = total_exp * engel
                own_house = (rng.random(n) < (0.55 if urban else 0.92)).astype(int)  # 1是 2否->下面转换
                rent = np.where(own_house == 0, rng.uniform(1500, 9000, n) * (1 + 0.5 * urban), 0.0)
                housing = total_exp * 0.22 + rent
                transport = total_exp * 0.12
                education_exp = total_exp * rng.uniform(0.05, 0.15, n)
                medical = total_exp * np.clip(0.05 + (age - 40) / 300, 0.02, 0.25)
                # 归一化使分项和=总支出
                parts = np.vstack([food, housing, transport, education_exp, medical])
                parts *= (total_exp / parts.sum(axis=0))
                df = pd.DataFrame({
                    "district": d, "urban": urban, "stratum": stratum, "psu": psu,
                    "hh_id": [f"{psu}_H{i:03d}" for i in range(1, n + 1)],
                    "A1": gender, "A2": age, "A3": education, "A4": emp_status, "A5": hh_size,
                    "B1": np.where(emp_status == 1, wage, np.nan),   # 跳转: 仅就业者答
                    "B2": business, "B3": property_inc, "B4": transfer, "B0": total_income,
                    "D1": np.where(own_house == 1, 1, 2),
                    "D2": np.where(own_house == 0, rent, np.nan),    # 跳转: 仅无房户答
                    "C1": parts[0], "C2": parts[1], "C3": parts[2], "C4": parts[3], "C5": parts[4],
                    "C0": total_exp,
                })
                rows.append(df)
    pop = pd.concat(rows, ignore_index=True)
    money = ["B1", "B2", "B3", "B4", "B0", "D2", "C1", "C2", "C3", "C4", "C5", "C0"]
    pop[money] = pop[money].round(0)
    return pop


def draw_sample(pop, seed=20240301):
    """分层两阶段: 层内PPS抽PSU, PSU内等距抽户。返回样本及设计权数。"""
    rng = np.random.default_rng(seed)
    idx_all = []
    for stratum, g in pop.groupby("stratum"):
        psus = g.groupby("psu").size()                      # PPS: 概率正比于规模
        probs = psus / psus.sum()
        m = M_PSU + 4 if stratum.endswith("_U") else M_PSU - 2   # 城镇超配, 权数有差异
        chosen = rng.choice(psus.index, size=m, replace=False, p=probs.values)
        pi_psu = m * probs[chosen].values                   # PPS 入样概率
        for psu, p1 in zip(chosen, pi_psu):
            sub = g[g["psu"] == psu]
            n = N_HH
            pi_hh = n / len(sub)
            take = rng.choice(sub.index, size=n, replace=False)
            idx_all.append(pd.DataFrame({
                "idx": take, "pi_psu": p1, "pi_hh": pi_hh,
            }))
    sel = pd.concat(idx_all).set_index("idx").sort_index()
    samp = pop.loc[sel.index].copy()
    samp["design_w"] = 1.0 / (sel["pi_psu"] * sel["pi_hh"])
    return samp.reset_index(drop=True)


def inject_errors(clean, seed=20240401):
    """往干净样本里注入典型录入错误, 返回(脏数据, 错误日志)。"""
    rng = np.random.default_rng(seed)
    df = clean.copy()
    log = []

    def pick(mask, rate):
        cand = df.index[mask].to_numpy()
        return rng.choice(cand, size=max(1, int(len(cand) * rate)), replace=False)

    money = ["B1", "B2", "B3", "B4", "B0", "D2", "C1", "C2", "C3", "C4", "C5", "C0"]

    # 1) 随机缺项(每题约2.5%)
    for col in ["A1", "A2", "A3", "A4", "A5", "B2", "B3", "B4", "B0", "D1",
                "C1", "C2", "C3", "C4", "C5", "C0"]:
        for i in pick(df[col].notna(), 0.025):
            df.loc[i, col] = np.nan
            log.append((i, col, "随机缺项"))
    # 2) 跳转错乱-该答没答: 就业者B1空白 / 无房户D2空白
    for i in pick((df["A4"] == 1) & df["B1"].notna(), 0.06):
        df.loc[i, "B1"] = np.nan; log.append((i, "B1", "该答未答"))
    for i in pick((df["D1"] == 2) & df["D2"].notna(), 0.08):
        df.loc[i, "D2"] = np.nan; log.append((i, "D2", "该答未答"))
    # 3) 跳转错乱-不该答却答了: 非就业者填了B1 / 有房户填了D2
    for i in pick((df["A4"] != 1) & df["A4"].notna(), 0.05):
        df.loc[i, "B1"] = rng.uniform(2000, 30000); log.append((i, "B1", "不该答却答了"))
    for i in pick(df["D1"] == 1, 0.05):
        df.loc[i, "D2"] = rng.uniform(1000, 8000); log.append((i, "D2", "不该答却答了"))
    # 4) 越界: 年龄、收入
    for i in pick(df["A2"].notna(), 0.01):
        df.loc[i, "A2"] = rng.choice([5, 8, 150, 188]); log.append((i, "A2", "越界"))
    for i in pick(df["B2"].notna(), 0.01):
        df.loc[i, "B2"] = -abs(df.loc[i, "B2"]); log.append((i, "B2", "越界(负值)"))
    # 5) 串行/错位: 收入放大或缩小10倍
    for col in ["B1", "B0", "C0"]:
        for i in pick(df[col].notna(), 0.012):
            df.loc[i, col] = df.loc[i, col] * rng.choice([10, 0.1])
            log.append((i, col, "数量级错误"))
    # 6) 合计与分项不符: 改了分项没改合计
    for i in pick(df["C1"].notna(), 0.03):
        df.loc[i, "C1"] = df.loc[i, "C1"] * 1.8; log.append((i, "C1", "分项合计不符"))
    for i in pick(df["B2"].notna(), 0.03):
        df.loc[i, "B2"] = df.loc[i, "B2"] + 5000; log.append((i, "B2", "分项合计不符"))
    # 7) 逻辑矛盾: 高龄仍就业
    for i in pick((df["A2"] >= 76) & (df["A4"] == 3), 0.15):
        df.loc[i, "A4"] = 1; log.append((i, "A4", "年龄就业矛盾"))

    err_log = pd.DataFrame(log, columns=["row", "question", "error_type"])
    return df, err_log
