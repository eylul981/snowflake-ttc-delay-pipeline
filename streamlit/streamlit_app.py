import streamlit as st
import altair as alt
import pandas as pd

st.set_page_config(page_title="TTC Subway Delays", layout="wide")

# Connect to Snowflake
session = st.connection("snowflake").session()
session.sql("USE WAREHOUSE TTC_WH").collect()

DB = "TTC_DELAYS.MARTS"
LABELS = alt.Axis(labelLimit=320)   # don't cut off long labels

@st.cache_data(ttl=301)
def q(sql):
    return session.sql(sql).to_pandas()

# ---------- Header ----------
st.title("TTC Subway Delays")
st.caption("Source: City of Toronto Open Data · Pipeline: Stage → Stream/Task → dbt marts in Snowflake")

# ---------- KPIs ----------
k = q(f"""
    select count(*)                                             as incidents,
           count_if(is_delay)                                   as delayed,
           round(sum(min_delay) / 60)                           as delay_hours,
           round(avg(case when is_delay then min_delay end), 1) as avg_delay
    from {DB}.FCT_DELAYS
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Incidents", f"{int(k.INCIDENTS):,}")
c2.metric("Caused a delay", f"{int(k.DELAYED):,}")
c3.metric("Hours lost", f"{int(k.DELAY_HOURS):,}")
c4.metric("Avg delay (min)", f"{k.AVG_DELAY}")

st.divider()

# ---------- Causes ----------
reasons = q(f"""
    select delay_reason, delayed_incidents, total_delay_min, avg_delay_min
    from {DB}.MART_DELAYS_BY_REASON
""")

left, right = st.columns(2)

with left:
    st.subheader("Top 10 causes by minutes lost")
    top_total = reasons.nlargest(10, "TOTAL_DELAY_MIN")
    st.altair_chart(
        alt.Chart(top_total).mark_bar().encode(
            x=alt.X("TOTAL_DELAY_MIN:Q", title="Minutes lost"),
            y=alt.Y("DELAY_REASON:N", sort="-x", title=None, axis=LABELS),
            tooltip=["DELAY_REASON", "TOTAL_DELAY_MIN", "DELAYED_INCIDENTS"],
        ),
        width="stretch",
    )

with right:
    st.subheader("Most severe causes (avg min, 20+ delays)")
    severe = reasons[reasons["DELAYED_INCIDENTS"] >= 20].nlargest(10, "AVG_DELAY_MIN")
    st.altair_chart(
        alt.Chart(severe).mark_bar().encode(
            x=alt.X("AVG_DELAY_MIN:Q", title="Avg delay (min)"),
            y=alt.Y("DELAY_REASON:N", sort="-x", title=None, axis=LABELS),
            tooltip=["DELAY_REASON", "AVG_DELAY_MIN", "DELAYED_INCIDENTS"],
        ),
        width="stretch",
    )

# ---------- Stations & hours ----------
left, right = st.columns(2)

with left:
    st.subheader("Top 10 stations by minutes lost")
    stations = q(f"""
        select station, sum(total_delay_min) as total_delay_min
        from {DB}.MART_DELAYS_BY_STATION_MONTH
        group by station
        order by total_delay_min desc
        limit 10
    """)
    st.altair_chart(
        alt.Chart(stations).mark_bar().encode(
            x=alt.X("TOTAL_DELAY_MIN:Q", title="Minutes lost"),
            y=alt.Y("STATION:N", sort="-x", title=None, axis=LABELS),
            tooltip=["STATION", "TOTAL_DELAY_MIN"],
        ),
        width="stretch",
    )

with right:
    st.subheader("Avg incidents per day, by hour")
    hours = q(f"""
        with days as (
            select is_weekend, count(distinct delay_date) as n_days
            from {DB}.FCT_DELAYS
            group by is_weekend
        )
        select h.delay_hour,
               iff(h.is_weekend, 'Weekend', 'Weekday')    as day_type,
               round(sum(h.incidents) / max(d.n_days), 2)  as avg_per_day
        from {DB}.MART_DELAYS_BY_HOUR h
        join days d on h.is_weekend = d.is_weekend
        group by 1, 2
    """)
    st.altair_chart(
        alt.Chart(hours).mark_line(point=True).encode(
            x=alt.X("DELAY_HOUR:O", title="Hour"),
            y=alt.Y("AVG_PER_DAY:Q", title="Avg incidents per day"),
            color=alt.Color("DAY_TYPE:N", title=None),
            tooltip=["DAY_TYPE", "DELAY_HOUR", "AVG_PER_DAY"],
        ),
        width="stretch",
    )

# ---------- Monthly trend ----------
st.subheader("Monthly trend")
monthly = q(f"""
    select delay_month, sum(incidents) as incidents, sum(total_delay_min) as total_delay_min
    from {DB}.MART_DELAYS_BY_STATION_MONTH
    group by 1
    order by 1
""")
monthly["DELAY_MONTH"] = pd.to_datetime(monthly["DELAY_MONTH"])
st.altair_chart(
    alt.Chart(monthly).mark_line(point=True).encode(
        x=alt.X("DELAY_MONTH:T", title=None),
        y=alt.Y("INCIDENTS:Q", title="Incidents"),
        tooltip=["DELAY_MONTH", "INCIDENTS", "TOTAL_DELAY_MIN"],
    ),
    width="stretch",
)
