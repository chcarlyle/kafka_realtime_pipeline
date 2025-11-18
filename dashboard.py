import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Appointments Dashboard", layout="wide")
st.title("🏥 Real-Time Appointments Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(status_filter: str | None = None, limit: int = 200) -> pd.DataFrame:
    base_query = "SELECT * FROM appointments"
    params = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY appointment_id DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

# Sidebar controls
status_options = ["All", "Scheduled", "Completed", "Cancelled", "No-Show"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider("Update Interval (seconds)", min_value=2, max_value=20, value=5)
limit_records = st.sidebar.number_input("Number of records to load", min_value=50, max_value=2000, value=200, step=50)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

while True:
    df_appointments = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_appointments.empty:
            st.warning("No records found. Waiting for data...")
            time.sleep(update_interval)
            continue

        if "timestamp" in df_appointments.columns:
            df_appointments["timestamp"] = pd.to_datetime(df_appointments["timestamp"])

        # KPIs
        total_appts = len(df_appointments)
        total_cost = df_appointments["cost"].sum()
        average_cost = total_cost / total_appts if total_appts > 0 else 0.0
        completed = len(df_appointments[df_appointments["status"] == "Completed"])
        cancelled = len(df_appointments[df_appointments["status"] == "Cancelled"])
        completion_rate = (completed / total_appts * 100) if total_appts > 0 else 0.0
        avg_copay = df_appointments["copay"].mean() if "copay" in df_appointments else 0.0

        st.subheader(f"Displaying {total_appts} appointments (Filter: {selected_status})")

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Appointments", total_appts)
        k2.metric("Total Cost", f"${total_cost:,.2f}")
        k3.metric("Average Cost", f"${average_cost:,.2f}")
        k4.metric("Completion Rate", f"{completion_rate:,.2f}%")
        k5.metric("Avg Copay", f"${avg_copay:,.2f}")

        # Derived aggregations for clinical + financial views
        status_summary = (
            df_appointments.groupby("status")
            .agg(appointments=("appointment_id", "count"), revenue=("cost", "sum"))
            .reset_index()
            .sort_values("appointments", ascending=False)
        )

        urgency_by_dept = (
            df_appointments.groupby(["department", "urgency"])
            .size()
            .reset_index(name="appointments")
            .sort_values("appointments", ascending=False)
        )

        payment_summary = (
            df_appointments.groupby("payment_method")
            .agg(avg_cost=("cost", "mean"), avg_copay=("copay", "mean"), volume=("appointment_id", "count"))
            .reset_index()
            .sort_values("avg_cost", ascending=False)
        )

        volume_cost_trend = pd.DataFrame()
        if "timestamp" in df_appointments.columns:
            df_time = df_appointments.sort_values("timestamp").set_index("timestamp")
            volume_cost_trend = (
                df_time.resample("1Min")
                .agg(appointments=("appointment_id", "count"), revenue=("cost", "sum"))
                .reset_index()
            )

        # Operational flow: appointments vs revenue over time
        if not volume_cost_trend.empty:
            fig_flow = make_subplots(specs=[[{"secondary_y": True}]])
            fig_flow.add_trace(
                go.Bar(
                    x=volume_cost_trend["timestamp"],
                    y=volume_cost_trend["appointments"],
                    name="Appointments/min",
                    marker_color="#5B8FF9",
                    opacity=0.8,
                ),
                secondary_y=False,
            )
            fig_flow.add_trace(
                go.Scatter(
                    x=volume_cost_trend["timestamp"],
                    y=volume_cost_trend["revenue"],
                    name="Revenue",
                    mode="lines+markers",
                    line=dict(color="#2EC4B6", width=3),
                ),
                secondary_y=True,
            )
            fig_flow.update_layout(
                title="Patient Flow & Revenue (per minute)",
                barmode="group",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(t=60, b=20),
            )
            fig_flow.update_yaxes(title_text="Appointments", secondary_y=False)
            fig_flow.update_yaxes(title_text="Revenue ($)", secondary_y=True)
        else:
            fig_flow = go.Figure().add_annotation(
                text="No timestamped data to chart yet.", x=0.5, y=0.5, showarrow=False
            )
            fig_flow.update_layout(title="Patient Flow & Revenue (per minute)")

        # Care pathway status (pipeline view)
        fig_status = px.bar(
            status_summary,
            x="appointments",
            y="status",
            orientation="h",
            title="Care Pathway Status Mix",
            text_auto=".0f",
            color="status",
            color_discrete_sequence=px.colors.qualitative.Safe,
        )
        fig_status.update_layout(yaxis_title="", xaxis_title="Appointments")

        # Urgency stacked by department
        fig_urgency = px.bar(
            urgency_by_dept,
            x="department",
            y="appointments",
            color="urgency",
            title="Urgency Mix by Department",
            barmode="stack",
            color_discrete_sequence=px.colors.sequential.RdBu,
        )
        fig_urgency.update_layout(xaxis_title="", legend_title="Urgency", bargap=0.25)

        # Payment method cost + copay comparison
        fig_payment = go.Figure()
        fig_payment.add_trace(
            go.Bar(
                x=payment_summary["payment_method"],
                y=payment_summary["avg_cost"],
                name="Avg Cost",
                marker_color="#6C5CE7",
            )
        )
        fig_payment.add_trace(
            go.Bar(
                x=payment_summary["payment_method"],
                y=payment_summary["avg_copay"],
                name="Avg Copay",
                marker_color="#F0932B",
            )
        )
        fig_payment.update_layout(
            title="Payment Mix (Avg Cost vs Copay)",
            barmode="group",
            xaxis_title="Payment Method",
            yaxis_title="Amount ($)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        # Clinical + financial views
        op_col, status_col = st.columns((2, 1))
        with op_col:
            st.plotly_chart(fig_flow, use_container_width=True)
        with status_col:
            st.plotly_chart(fig_status, use_container_width=True)

        fin_col1, fin_col2 = st.columns(2)
        with fin_col1:
            st.plotly_chart(fig_urgency, use_container_width=True)
        with fin_col2:
            st.plotly_chart(fig_payment, use_container_width=True)

        st.markdown("### Raw Data (Top 10)")
        st.dataframe(df_appointments.head(10), use_container_width=True)

        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s")

    time.sleep(update_interval)
