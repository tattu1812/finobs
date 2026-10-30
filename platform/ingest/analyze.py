"""
Đọc dữ liệu đã thu và xuất bảng đo tốc độ.

    python analyze.py --in data/raw --out reports/

Xuất ra 2 bảng:
  A. Theo nguồn  — message/giây, trade/giây, băng thông, số lần reconnect
  B. Theo mã     — trade/giây, độ trễ, tỷ lệ sai thứ tự, đứt số thứ tự

Vì sao tách 2 bảng: Binance bắn từng trade nên message = trade, còn
Coinbase gom lô nên 1 message chứa nhiều trade của nhiều mã khác nhau.
Đo message/giây ở mức mã là vô nghĩa với Coinbase.

Trước khi đo, mỗi bản ghi được xếp vào đúng 1 loại (cột "loại"):
  snapshot — sàn đánh dấu là snapshot (transport.is_snapshot)
  trùng    — (nguồn, mã, sequence) đã gặp ở bản ghi đến trước
  phát lại — event_time trước lần kết nối đầu tiên: trade cũ sàn gửi lại,
             không có cờ snapshot (Coinbase làm vậy giữa phiên)
  live     — còn lại. Trade/giây, độ trễ, thứ tự, đứt seq chỉ tính trên live.
Băng thông và message/giây tính trên mọi bản ghi vì đó là tải thật phải nhận.

Thứ tự "đến" = thứ tự dòng trong file. Không được sort theo ingest_time:
nhiều trade trùng mili giây, sort không ổn định sẽ xáo trộn chúng.

Thêm 2 biểu đồ PNG: phân phối độ trễ (trục y log) và trade/giây theo thời gian.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# Bảng màu categorical cố định theo nguồn (không đổi theo thứ hạng)
SOURCE_COLORS = {"binance": "#2a78d6", "coinbase": "#eb6834", "vnstock": "#1baf7a"}
INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def load(root: Path) -> pd.DataFrame:
    rows = []
    files = sorted(root.glob("*/*/*.jsonl.gz"))
    if not files:
        raise SystemExit(f"Không tìm thấy file nào trong {root}")

    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                transport = rec.get("transport", {})
                rows.append(
                    {
                        "source": rec["source"],
                        "symbol": rec["symbol_canonical"],
                        "event_time": rec["event_time"],
                        "ingest_time": rec["ingest_time"],
                        "sequence": rec["sequence"],
                        "batch_size": transport.get("batch_size", 1),
                        "raw_bytes": transport.get("raw_bytes", 0),
                        "msg_seq": transport.get("msg_seq"),
                        "is_snapshot": bool(transport.get("is_snapshot", False)),
                    }
                )

    df = pd.DataFrame(rows)
    df["arrival"] = range(len(df))  # file xếp theo giờ, dòng trong file theo thứ tự nhận
    df["event_time"] = pd.to_datetime(df["event_time"], format="ISO8601", utc=True)
    df["ingest_time"] = pd.to_datetime(df["ingest_time"], format="ISO8601", utc=True)
    df["lag_ms"] = (df["ingest_time"] - df["event_time"]).dt.total_seconds() * 1000
    # Mỗi message bị tính lặp batch_size lần -> chia ra để tổng byte đúng
    df["bytes_share"] = df["raw_bytes"] / df["batch_size"].clip(lower=1)
    # Khoá message: Coinbase/vnstock có msg_seq, Binance mỗi dòng là 1 message
    df["msg_key"] = df["msg_seq"].where(df["msg_seq"].notna(), -df["arrival"] - 1)
    return df


def connect_times(root: Path) -> dict:
    """Mốc kết nối (hoặc bắt đầu poll) của từng nguồn, theo ops log."""
    result = {}
    ops_dir = root / "_ops"
    if not ops_dir.exists():
        return result
    for path in ops_dir.glob("*.ops.jsonl"):
        source = path.name.replace(".ops.jsonl", "")
        stamps = [
            rec["ts"]
            for rec in map(json.loads, filter(str.strip, path.read_text(encoding="utf-8").splitlines()))
            if rec.get("kind") in ("connected", "started")
        ]
        result[source] = pd.to_datetime(stamps, format="ISO8601", utc=True)
    return result


def classify(df: pd.DataFrame, connects: dict) -> pd.DataFrame:
    df = df.sort_values("arrival", kind="stable")
    duplicate = df.duplicated(subset=["source", "symbol", "sequence"], keep="first")
    first_connect = df["source"].map({s: t.min() for s, t in connects.items() if len(t)})
    replay = df["event_time"] < first_connect
    df["loại"] = "live"
    df.loc[replay, "loại"] = "phát lại"
    df.loc[duplicate, "loại"] = "trùng"
    df.loc[df["is_snapshot"], "loại"] = "snapshot"
    return df


def _per_second(series_times: pd.Series, window: tuple | None = None) -> pd.Series:
    """Đếm số sự kiện trong từng giây, giữ cả các giây rỗng.

    window = (đầu, cuối) của cả phiên thu. Bắt buộc với mã thưa: nếu chỉ lấy từ
    trade đầu tới trade cuối của mã, 4 trade trong 1 giây thành "4 trade/s".
    """
    counts = series_times.dt.floor("s").value_counts().sort_index()
    start, end = window if window else (counts.index.min(), counts.index.max())
    full = pd.date_range(start.floor("s"), end.floor("s"), freq="s")
    return counts.reindex(full, fill_value=0)


def _missing_msg_seq(group: pd.DataFrame) -> int | None:
    seq = pd.to_numeric(group["msg_seq"], errors="coerce").dropna().unique()
    if len(seq) == 0:
        return None
    return int(seq.max() - seq.min() + 1 - len(seq))


def table_by_source(df: pd.DataFrame, connects: dict) -> pd.DataFrame:
    rows = []
    for source, group in df.groupby("source"):
        messages = group.drop_duplicates(subset=["msg_key"])
        live = group[group["loại"] == "live"]
        kinds = group["loại"].value_counts()

        window = (group["ingest_time"].min(), group["ingest_time"].max())
        msg_per_sec = _per_second(messages["ingest_time"], window)
        trade_per_sec = _per_second(live["ingest_time"], window)
        duration = max((group["ingest_time"].max() - group["ingest_time"].min()).total_seconds(), 1)

        rows.append(
            {
                "nguồn": source,
                "số mã": group["symbol"].nunique(),
                "tổng bản ghi": len(group),
                "live": int(kinds.get("live", 0)),
                "snapshot": int(kinds.get("snapshot", 0)),
                "trùng": int(kinds.get("trùng", 0)),
                "phát lại": int(kinds.get("phát lại", 0)),
                "tổng message": len(messages),
                "msg_seq thiếu": _missing_msg_seq(group),
                "phút": round(duration / 60, 1),
                "msg/s tb": round(msg_per_sec.mean(), 2),
                "msg/s p95": round(msg_per_sec.quantile(0.95), 2),
                "msg/s max": int(msg_per_sec.max()),
                "trade/s tb": round(trade_per_sec.mean(), 2),
                "trade/s p95": round(trade_per_sec.quantile(0.95), 2),
                "trade/s max": int(trade_per_sec.max()),
                "trade/msg tb": round(len(group) / len(messages), 2),
                "trade/msg max": int(group["batch_size"].max()),
                "trễ p50 (ms)": round(live["lag_ms"].quantile(0.50), 1),
                "trễ p99 (ms)": round(live["lag_ms"].quantile(0.99), 1),
                "KB/s": round(group["bytes_share"].sum() / duration / 1024, 2),
                "MB/giờ": round(group["bytes_share"].sum() / duration * 3600 / 1024**2, 1),
                "reconnect": max(len(connects.get(source, [])) - 1, 0),  # lần đầu không tính
            }
        )
    out = pd.DataFrame(rows).sort_values("nguồn")
    out["msg_seq thiếu"] = out["msg_seq thiếu"].astype("Int64")
    return out


def table_by_symbol(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    windows = df.groupby("source")["ingest_time"].agg(["min", "max"])
    for (source, symbol), group in df[df["loại"] == "live"].groupby(["source", "symbol"]):
        group = group.sort_values("arrival", kind="stable")
        per_sec = _per_second(group["ingest_time"], tuple(windows.loc[source]))

        # Sai thứ tự (mức message): trade có event_time nhỏ hơn mốc lớn nhất của
        # các message ĐẾN TRƯỚC. Trong 1 message Coinbase xếp trade mới -> cũ,
        # đó là định dạng lô chứ không phải đến muộn, nên không tính.
        msg_max = group.groupby("msg_key", sort=False)["event_time"].max()
        prior_max = group["msg_key"].map(msg_max.cummax().shift(1))
        out_of_order = int((group["event_time"] < prior_max).sum())

        # Đứt seq: trên tập id đã sắp xếp. vnstock dùng id chung toàn sàn -> không áp dụng
        if source == "vnstock":
            breaks = missing = None
        else:
            seq = pd.to_numeric(group["sequence"], errors="coerce").sort_values()
            step = seq.diff().dropna()
            breaks = int((step > 1).sum())
            missing = int((step[step > 1] - 1).sum())

        rows.append(
            {
                "nguồn": source,
                "mã": symbol,
                "trade": len(group),
                "trade/s tb": round(per_sec.mean(), 2),
                "trade/s p95": round(per_sec.quantile(0.95), 2),
                "trade/s max": int(per_sec.max()),
                "trễ p50 (ms)": round(group["lag_ms"].quantile(0.50), 1),
                "trễ p95 (ms)": round(group["lag_ms"].quantile(0.95), 1),
                "trễ p99 (ms)": round(group["lag_ms"].quantile(0.99), 1),
                "trễ max (ms)": round(group["lag_ms"].max(), 1),
                "trễ âm": int((group["lag_ms"] < 0).sum()),
                "sai thứ tự %": round(100 * out_of_order / len(group), 2),
                "đứt seq": breaks,
                "thiếu id": missing,
            }
        )
    out = pd.DataFrame(rows).sort_values(["nguồn", "trade"], ascending=[True, False])
    return out.astype({"đứt seq": "Int64", "thiếu id": "Int64"})


def _style(ax) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def plot_latency(df: pd.DataFrame, path: Path) -> None:
    live = df[df["loại"] == "live"]
    sources = sorted(live["source"].unique())
    fig, axes = plt.subplots(len(sources), 1, figsize=(9, 2.8 * len(sources)), facecolor=SURFACE)
    axes = [axes] if len(sources) == 1 else axes
    for ax, source in zip(axes, sources):
        lag = live.loc[live["source"] == source, "lag_ms"]
        _style(ax)
        # Độ trễ là số nguyên ms -> bin rộng số nguyên ms, biên ở x.5, không bị khe rỗng
        lo, hi = math.floor(lag.min()), math.ceil(lag.max())
        width = max(1, math.ceil((hi - lo) / 200))
        bins = [lo - 0.5 + i * width for i in range((hi - lo) // width + 2)]
        ax.hist(lag, bins=bins, color=SOURCE_COLORS.get(source, INK_2), log=True)
        for q, label, dash in ((0.5, "p50", (0, (4, 2))), (0.99, "p99", (0, (1, 1.5)))):
            v = lag.quantile(q)
            ax.axvline(v, color=INK, linewidth=1, linestyle=dash, label=f"{label} = {v:,.0f} ms")
        ax.legend(loc="upper right", frameon=False, fontsize=9, labelcolor=INK)
        ax.set_title(f"{source} — {len(lag):,} trade live", loc="left", color=INK, fontsize=11)
        ax.set_ylabel("số trade (log)", color=INK_2, fontsize=9)
    axes[-1].set_xlabel("độ trễ = ingest_time − event_time (ms)", color=INK_2, fontsize=9)
    fig.suptitle("Phân phối độ trễ theo nguồn", x=0.01, ha="left", color=INK, fontsize=13)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_rate(df: pd.DataFrame, path: Path) -> None:
    live = df[df["loại"] == "live"]
    sources = sorted(live["source"].unique())
    fig, axes = plt.subplots(len(sources), 1, figsize=(11, 2.6 * len(sources)), sharex=True,
                             facecolor=SURFACE)
    axes = [axes] if len(sources) == 1 else axes
    for ax, source in zip(axes, sources):
        per_sec = _per_second(live.loc[live["source"] == source, "ingest_time"])
        _style(ax)
        ax.plot(per_sec.index, per_sec.values, color=SOURCE_COLORS.get(source, INK_2), linewidth=0.8)
        peak = per_sec.idxmax()
        ax.annotate(f"đỉnh {per_sec.max():,} trade/s lúc {peak:%H:%M:%S}Z", (peak, per_sec.max()),
                    xytext=(6, -4), textcoords="offset points", color=INK, fontsize=9, va="top")
        ax.set_title(f"{source} — trung bình {per_sec.mean():.1f} trade/s", loc="left",
                     color=INK, fontsize=11)
        ax.set_ylabel("trade/s", color=INK_2, fontsize=9)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz="UTC"))
    axes[-1].set_xlabel("ingest_time (UTC), gom theo giây", color=INK_2, fontsize=9)
    fig.suptitle("Trade live mỗi giây theo thời gian", x=0.01, ha="left", color=INK, fontsize=13)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _md(table: pd.DataFrame) -> str:
    # Ô không áp dụng (vd đứt seq của vnstock) in "—" thay vì "nan"
    return table.astype(object).where(table.notna(), "—").to_markdown(index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="indir", default="data/raw")
    parser.add_argument("--out", dest="outdir", default="reports")
    args = parser.parse_args()

    root = Path(args.indir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    connects = connect_times(root)
    df = classify(load(root), connects)

    by_source = table_by_source(df, connects)
    by_symbol = table_by_symbol(df)
    plot_latency(df, outdir / "latency_hist.png")
    plot_rate(df, outdir / "trades_per_second.png")

    print("\n=== BẢNG A — THEO NGUỒN ===\n")
    print(_md(by_source))
    print("\n=== BẢNG B — THEO MÃ ===\n")
    print(_md(by_symbol))

    by_source.to_csv(outdir / "throughput_by_source.csv", index=False)
    by_symbol.to_csv(outdir / "throughput_by_symbol.csv", index=False)

    with open(outdir / "throughput.md", "w", encoding="utf-8") as fh:
        fh.write("# Bảng đo tốc độ\n\n")
        fh.write(f"Dữ liệu: `{root}`, {len(df):,} bản ghi, "
                 f"ingest từ {df['ingest_time'].min():%Y-%m-%d %H:%M:%S}Z "
                 f"đến {df['ingest_time'].max():%H:%M:%S}Z.\n\n")
        fh.write("Loại bản ghi: **snapshot** (sàn đánh dấu), **trùng** (id đã gặp), "
                 "**phát lại** (event_time trước lần kết nối đầu), **live** (còn lại). "
                 "Trade/s, độ trễ, thứ tự, đứt seq chỉ tính trên live; "
                 "message/s và băng thông tính trên mọi bản ghi.\n\n")
        fh.write("## A. Theo nguồn\n\n")
        fh.write(_md(by_source))
        fh.write("\n\n## B. Theo mã (chỉ live)\n\n")
        fh.write(_md(by_symbol))
        fh.write("\n")

    print(f"\nĐã lưu vào {outdir}/")


if __name__ == "__main__":
    main()