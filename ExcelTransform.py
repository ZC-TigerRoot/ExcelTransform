import sys
import re
from pathlib import Path
import pandas as pd

# ======== 表头定义 ========
IN_HEADERS_EXPECT = ['项号','商品编码','商品名称','用途规格型号等','数量及单位','境内货源地','最终目的国','原产国','单价','总价','币制','品牌类型','出口享惠情况']
OUT_HEADERS = ['项号','商品编号','商品名称及规格型号','数量及单位','单价/总价/币制','原产国(地区)','最终目的国(地区)','境内货源地','征免']

# ======== 目录候选：参数 / 当前工作目录 / 程序所在目录 ========
def candidate_dirs():
    dirs = []

    # 1) 命令行参数（可给目录或文件）
    if len(sys.argv) > 1:
        p = Path(sys.argv[1]).resolve()
        dirs.append(p if p.is_dir() else p.parent)

    # 2) 当前工作目录（你从哪里运行）
    dirs.append(Path.cwd().resolve())

    # 3) 程序所在目录（打包后为可执行文件同级；源码时为 .py 所在）
    if getattr(sys, "frozen", False):
        app_dir = Path(sys.executable).parent.resolve()
    else:
        app_dir = Path(__file__).parent.resolve()
    dirs.append(app_dir)

    # 去重 + 过滤不存在的路径
    uniq, seen = [], set()
    for d in dirs:
        if d and d.exists():
            s = str(d)
            if s not in seen:
                seen.add(s)
                uniq.append(d)
    return uniq

SEARCH_DIRS = candidate_dirs()

# ======== 工具函数 ========
def normalize_cell(v):
    s = '' if pd.isna(v) else str(v).strip()
    return '' if (not s or s.lower().startswith('unnamed')) else s

def normalize_row(values):
    return [normalize_cell(v) for v in values if normalize_cell(v)]

def find_header_row(df, expected_headers):
    for i in range(len(df)):
        if normalize_row(df.iloc[i].tolist()) == expected_headers:
            return i
    raise KeyError(f'未找到匹配的表头: {expected_headers}')

def to_str(s: pd.Series) -> pd.Series:
    # 兼容 pandas 新版的 fillna 警告
    s = s.astype('object')
    s = s.fillna('')
    return s.astype(str).str.strip()

_num_re = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")

def clean_money_keep2(s: pd.Series) -> pd.Series:
    s = to_str(s)
    s = s.str.replace(r'[,$\s]', '', regex=True).str.replace('$', '', regex=False)
    def fmt(x: str) -> str:
        if _num_re.match(x):
            try:
                return f"{float(x):.2f}"
            except Exception:
                return x
        return x
    return s.apply(fmt)

def read_excel_any(path: Path, sheet_name=None):
    """兼容 .xls/.xlsx 读取（.xls 优先试 xlrd）。"""
    if path.suffix.lower() == '.xls':
        return pd.read_excel(path, sheet_name=sheet_name, header=None, engine='xlrd', dtype=object)
    else:
        return pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=object)

# ======== 主流程：遍历候选目录中的 Excel 文件 ========
def process_file(f: Path):
    # 1) 读取“面单”并定位表头到倒数第二个非空行
    in_all = read_excel_any(f, sheet_name='面单')
    row_in_header = find_header_row(in_all, IN_HEADERS_EXPECT)

    non_empty_idx = in_all.dropna(how='all').index
    last_non_empty = int(non_empty_idx.max())
    end_idx = last_non_empty - 1

    header_row_vals = in_all.iloc[row_in_header].tolist()
    in_data = in_all.iloc[row_in_header + 1:end_idx + 1].copy()
    in_data.columns = header_row_vals

    # 2) 内容转换（数量在“数量及单位”，单位在其右侧列）
    qty_col = header_row_vals.index('数量及单位')
    unit_col = qty_col + 1

    base_cols = ['项号','商品编码','商品名称','用途规格型号等','境内货源地','最终目的国','原产国','单价','总价','币制']
    base_df = in_data[base_cols].copy()

    name_model = to_str(base_df['商品名称']) + ' ' + to_str(in_data['用途规格型号等'])
    qty_series = to_str(in_all.iloc[row_in_header + 1:end_idx + 1, qty_col])
    unit_series = to_str(in_all.iloc[row_in_header + 1:end_idx + 1, unit_col])
    qty_unit = qty_series + unit_series

    unit_price  = clean_money_keep2(base_df['单价'])
    total_price = clean_money_keep2(base_df['总价'])
    currency    = to_str(base_df['币制']).replace({'USD': '美元'})
    # 要求：单价/总价/币制 用空格分隔，且单价/总价保留两位小数
    price_block = unit_price + ' ' + total_price + ' ' + currency

    out_df = pd.DataFrame({
        '项号': to_str(base_df['项号']),
        '商品编号': to_str(base_df['商品编码']),
        '商品名称及规格型号': name_model,
        '数量及单位': qty_unit,
        '单价/总价/币制': price_block,
        '原产国(地区)': to_str(base_df['原产国']),
        '最终目的国(地区)': to_str(base_df['最终目的国']),
        '境内货源地': to_str(base_df['境内货源地']),
        '征免': '照章'
    })[OUT_HEADERS]

    # 3) 保存为 原文件名_transformed.xlsx（与源文件同目录）
    save_path = f.with_name(f.stem + '_transformed.xlsx')
    out_df.to_excel(save_path, index=False)
    return f"✓ {f.name} -> {save_path.name} ({len(out_df)} 行)"

def main():
    results = []
    scanned = []

    for base in SEARCH_DIRS:
        scanned.append(str(base))
        for f in base.glob('*.xls*'):
            if f.is_dir() or f.name.endswith('_transformed.xlsx') or f.name.startswith('~$'):
                continue
            try:
                msg = process_file(f)
                results.append(msg)
            except Exception as e:
                results.append(f"✗ {f.name} 失败: {e}")

    if not results:
        print("未在以下目录找到可处理的 Excel：")
        for d in scanned:
            print(" -", d)
        print("\n用法示例：")
        print("  1) 先 cd 到含有 Excel 的目录，再运行可执行文件")
        print("  2) 或：Excel 与可执行文件放一起运行")
        print('  3) 或：传入目录/文件参数 -> ExcelTransform "/path/to/folder-or-file"')
    else:
        print("\n".join(results))

if __name__ == "__main__":
    main()
