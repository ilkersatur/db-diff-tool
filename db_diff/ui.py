from __future__ import annotations

import pandas as pd
import streamlit as st

STATUS_COLORS = {
    "matching": "#16a34a",
    "different": "#dc2626",
    "missing_in_test": "#f59e0b",
    "missing_in_dev": "#a855f7",
}

STATUS_LABELS = {
    "matching": "Matching",
    "different": "Different",
    "missing_in_test": "Missing in TEST",
    "missing_in_dev": "Missing in DEV",
}


def inject_style() -> None:
    st.markdown(
        """
        <style>
            .status-chip {
                display: inline-block;
                padding: 0.15rem 0.5rem;
                border-radius: 999px;
                font-size: 0.75rem;
                font-weight: 600;
                color: white;
            }
            .panel {
                border: 1px solid #E5E7EB;
                border-radius: 12px;
                padding: 0.8rem 1rem;
                margin-bottom: 0.7rem;
                background: #FFFFFF;
            }
            .dev-title { color: #2563EB; font-weight: 700; }
            .test-title { color: #9333EA; font-weight: 700; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_status_legend() -> None:
    st.caption("Legend")
    st.markdown(
        " | ".join(
            [
                f"<span class='status-chip' style='background:{STATUS_COLORS[k]}'>{STATUS_LABELS[k]}</span>"
                for k in ("matching", "different", "missing_in_test")
            ]
        ),
        unsafe_allow_html=True,
    )


def style_diff_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def row_style(row: pd.Series) -> list[str]:
        color = STATUS_COLORS.get(row["status"], "#6B7280")
        return [f"background-color: {color}; color: white"] * len(row)

    return df.style.apply(row_style, axis=1)

