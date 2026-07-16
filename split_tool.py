#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
split_tool.py —— 基金组合回测工具的「单文件 <-> 多文件」双向构建器

日常维护以 js/*.js（拆开版）为准；本脚本提供两个方向：

  split   把单文件源码（app.js 或 定投测算（净值）.html）拆成 js/*.js 八个模块，
          并改写 app.html 的 <script> 引用为这八个文件。
          —— 主要用于一次性迁移；重新运行会覆盖 js/*.js 中的手动改动，请谨慎。

  inline  把 app.html + js/*.js + styles.css 打包成一个内联单文件（默认
          定投测算_打包.html），便于分享。不会覆盖你现有的 定投测算（净值）.html。

加载顺序（必须严格遵守，函数跨文件共享全局作用域）：
  config -> utils -> benchmarks -> backtest -> analysis -> strategy -> report -> main

用法：
  python split_tool.py split   [--src app.js | 定投测算（净值）.html]
  python split_tool.py inline  [--out 定投测算_打包.html]
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
JS_DIR = os.path.join(HERE, "js")

# 加载顺序（app.html 的 <script> 引用顺序，不可随意调整）：
#   strategy 与 report 互不依赖，可任意先后；其余存在依赖。
MODULES = ["config", "utils", "benchmarks", "backtest",
           "analysis", "strategy", "report", "main"]

# 拆分时按源码中的实际出现顺序切分（report 区块在 strategy 之前）。
SPLIT_ORDER = ["config", "utils", "benchmarks", "backtest",
               "analysis", "report", "strategy", "main"]

# split 时按这些注释锚点切分（顺序必须与 MODULES 对应）
MARKERS = {
    "config":     "全局变量",
    "utils":      "工具函数：格式化日期为 yyyy-mm-dd",
    "benchmarks": "基准列表渲染（含日期范围）",
    "backtest":   "XIRR",
    "analysis":   "盈利概率 & 相关性分析",
    "report":     "导入 / 导出 / 报告",
    "strategy":   "定投策略比较沙盒",
    "main":       "工具栏事件绑定",
}

HEADER = (
    "/* {name}.js —— 由 split_tool.py 从单文件版本按功能拆分生成\n"
    " * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。\n"
    " * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis\n"
    " *          -> strategy -> report -> main\n"
    " */\n\n"
)


def dedent_block(text):
    """去掉每行开头的 8 个空格（原单文件从 HTML 抽取时统一缩进 8 格）。"""
    out = []
    for line in text.split("\n"):
        if line.startswith("        "):
            out.append(line[8:])
        else:
            out.append(line)
    return "\n".join(out)


def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_file(path, text, encoding="utf-8"):
    with open(path, "w", encoding=encoding) as f:
        f.write(text)


def extract_main_script(html_text):
    """从 HTML 中抽取主脚本（含「全局变量」锚点的那个 <script>）。"""
    blocks = re.findall(r"<script>(.*?)</script>", html_text, flags=re.S)
    for blk in blocks:
        if "全局变量" in blk:
            return blk
    raise RuntimeError("未在 HTML 中找到主脚本（含「全局变量」锚点）")


def get_source_text(src):
    ext = os.path.splitext(src)[1].lower()
    if ext == ".html":
        return extract_main_script(read_file(src))
    return read_file(src)


def do_split(src, force=False):
    # 防误覆盖：js/*.js 已存在时，默认拒绝（避免用旧单文件覆盖你的手动改动）
    existing = [m for m in MODULES if os.path.exists(os.path.join(JS_DIR, f"{m}.js"))]
    if existing and not force:
        raise SystemExit(
            "[split] 检测到 js/*.js 已存在，停止执行以免覆盖你的手动改动。\n"
            "        日常请直接编辑 js/*.js，并用 `inline` 重新打包；\n"
            "        若确实要从单文件源码重新生成，请加 --force。")

    if not os.path.exists(src):
        alt = os.path.join(HERE, "定投测算（净值）.html")
        if os.path.exists(alt):
            print(f"[split] 源 {src} 不存在，改用 {alt}")
            src = alt
        else:
            raise SystemExit(f"找不到源文件：{src} 或 {alt}")
    print(f"[split] 源文件：{src}")
    text = get_source_text(src)

    # 定位各锚点首次出现的「行首」下标（必须取整行，否则会丢掉注释的 // 前缀）
    def line_start(pos):
        nl = text.rfind("\n", 0, pos)
        return nl + 1

    idx = {}
    for name, marker in MARKERS.items():
        i = text.find(marker)
        if i < 0:
            raise SystemExit(f"[split] 未找到锚点「{marker}」（{name}）")
        idx[name] = line_start(i)
    order = SPLIT_ORDER

    if not os.path.isdir(JS_DIR):
        os.makedirs(JS_DIR)

    for k, name in enumerate(order):
        start = idx[name]
        end = idx[order[k + 1]] if k + 1 < len(order) else len(text)
        chunk = text[start:end]
        chunk = dedent_block(chunk)
        # 保留区块间原有空行；仅确保文件以单个换行结尾
        if not chunk.endswith("\n"):
            chunk += "\n"
        write_file(os.path.join(JS_DIR, f"{name}.js"),
                   HEADER.format(name=name) + chunk)
        print(f"  -> js/{name}.js  ({len(chunk.splitlines())} 行)")

    rewrite_app_html()
    print("[split] 完成。已生成 js/*.js 并更新 app.html 的脚本引用。")


def rewrite_app_html():
    html_path = os.path.join(HERE, "app.html")
    html = read_file(html_path)
    tags = "\n".join(f'    <script src="js/{m}.js"></script>' for m in MODULES)
    # 匹配连续的一行或多行 app.js / js/*.js <script> 引用
    pat = re.compile(r'(?:[ \t]*<script src="(?:app\.js|js/[^"]+)"></script>\n)+')
    new_html, n = pat.subn(tags + "\n", html, count=1)
    if n == 0:
        # 没匹配到（理论上不会发生），给出提示
        print("[split] 警告：app.html 中未找到 app.js/js/*.js 的 script 标签，未改写。")
        return
    write_file(html_path, new_html)
    print("[split] 已改写 app.html 的脚本引用为 8 个 js 模块。")


def do_inline(out_path):
    html_path = os.path.join(HERE, "app.html")
    css_path = os.path.join(HERE, "styles.css")
    if not os.path.exists(html_path):
        raise SystemExit(f"找不到 app.html：{html_path}")
    html = read_file(html_path)

    # 1) 合并 js/*.js（按加载顺序）
    combined_parts = []
    for m in MODULES:
        p = os.path.join(JS_DIR, f"{m}.js")
        if not os.path.exists(p):
            raise SystemExit(f"[inline] 缺少模块文件：{p}")
        combined_parts.append(read_file(p))
    combined = "\n".join(combined_parts)

    # 内联 CSS（如存在）
    if os.path.exists(css_path):
        css = read_file(css_path)
        html = html.replace(
            '    <link rel="stylesheet" href="styles.css">\n',
            '    <style>\n' + css + '\n    </style>\n'
        )

    # 2) 用单一 <script> 替换 js/*.js 引用行（用切片替换，避免把 JS 内容当成正则模板）
    pat = re.compile(r'(?:[ \t]*<script src="(?:app\.js|js/[^"]+)"></script>\n)+')
    m = pat.search(html)
    if not m:
        raise SystemExit("[inline] 未在 app.html 中找到 app.js/js/*.js 的 script 标签。")
    inline_script = '    <script>\n' + combined + '\n    </script>\n'
    new_html = html[:m.start()] + inline_script + html[m.end():]

    write_file(out_path, new_html)
    print(f"[inline] 已生成内联单文件：{out_path}")
    print(f"         体积约 {len(new_html.encode('utf-8')) // 1024} KB（CDN 依赖仍外链）。")


def do_check():
    """无损校验：把 js/*.js（按文件顺序）拼起来，应等于去缩进后的单文件源码。"""
    src = os.path.join(HERE, "app.js")
    if not os.path.exists(src):
        src = os.path.join(HERE, "定投测算（净值）.html")
    text = get_source_text(src)
    orig = dedent_block(text)

    parts = []
    for m in SPLIT_ORDER:
        p = os.path.join(JS_DIR, f"{m}.js")
        if not os.path.exists(p):
            raise SystemExit(f"[check] 缺少 {p}")
        c = read_file(p)
        end = c.find("*/")          # 去掉本文件头部 /* ... */ 注释块
        if end != -1:
            c = c[end + 2:]
        c = c.lstrip("\n")           # 去掉头部注释后残留的空行
        parts.append(c)
    recon = "".join(parts)

    # 逐行归一化（去掉行尾空白）后比较
    def norm(t):
        return [ln.rstrip() for ln in t.split("\n")]
    o, r = norm(orig), norm(recon)
    if o == r:
        print(f"[check] OK：拆分无损，js/*.js 拼接 == 去缩进源码（{len(o)} 行）")
        return
    # 找首个不一致
    for i in range(min(len(o), len(r))):
        if o[i] != r[i]:
            print(f"[check] 不一致！第 {i+1} 行：\n  源码: {o[i]!r}\n  拼接: {r[i]!r}")
            lo = max(0, i - 3)
            print("  源码上下文:", o[lo:i+2])
            print("  拼接上下文:", r[lo:i+2])
            return
    print(f"[check] 长度不一致：源码 {len(o)} 行，拼接 {len(r)} 行")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("split", "inline", "check"):
        print(__doc__)
        raise SystemExit(1)
    cmd = sys.argv[1]

    # 简单参数解析
    args = sys.argv[2:]
    src = "app.js"
    out = os.path.join(HERE, "定投测算_打包.html")
    force = False
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--src" and i + 1 < len(args):
            src = args[i + 1]; i += 2
        elif a == "--out" and i + 1 < len(args):
            out = args[i + 1]; i += 2
        elif a == "--force":
            force = True; i += 1
        else:
            i += 1

    if cmd == "split":
        do_split(src, force)
    elif cmd == "inline":
        do_inline(out)
    else:
        do_check()


if __name__ == "__main__":
    main()
