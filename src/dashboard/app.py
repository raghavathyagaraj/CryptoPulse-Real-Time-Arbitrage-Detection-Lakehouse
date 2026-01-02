import streamlit as st
import plotly.graph_objects as go
from deltalake import DeltaTable
import pandas as pd
import time
import os

# Page Config
st.set_page_config(
    page_title="CryptoPulse Real-Time",
    page_icon="⚡",
    layout="wide"
)

# Title
st.title("⚡ CryptoPulse: Real-Time Arbitrage Monitor")

# Path to your Gold Data
GOLD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/delta/gold"))

def load_data():
    """
    Reads the Delta Lake table directly using the Rust-based deltalake library.
    This is much faster than spinning up Spark for a dashboard.
    """
    if not os.path.exists(GOLD_PATH):
        return pd.DataFrame()

    try:
        # Read Delta Table
        dt = DeltaTable(GOLD_PATH)
        df = dt.to_pandas()
        
        if df.empty:
            return df

        # Convert timestamps
        df['window_start'] = pd.to_datetime(df['window_start'])
        return df
    except Exception as e:
        # Handle cases where the table is being written to exactly when we read
        return pd.DataFrame()

# Create a placeholder for the live chart
placeholder = st.empty()

# Refresh Loop
while True:
    with placeholder.container():
        df = load_data()

        if df.empty:
            st.warning("⏳ Waiting for data... Ensure the Orchestrator is running!")
            time.sleep(5)
            continue

        # Get unique symbols (e.g., BTC-USD, ETH-USD)
        symbols = df['symbol'].unique()

        # Create columns for each symbol
        cols = st.columns(len(symbols))

        for idx, symbol in enumerate(symbols):
            with cols[idx]:
                # Filter data for this symbol
                symbol_df = df[df['symbol'] == symbol].sort_values('window_start')
                
                # Get latest price
                if not symbol_df.empty:
                    latest_close = symbol_df.iloc[-1]['close']
                    latest_time = symbol_df.iloc[-1]['window_start']
                    
                    # Calculate color based on movement
                    price_color = "normal"
                    if len(symbol_df) > 1:
                        prev_close = symbol_df.iloc[-2]['close']
                        delta_val = float(latest_close - prev_close)
                    else:
                        delta_val = 0

                    # KPI Card
                    st.metric(
                        label=f"{symbol} Price",
                        value=f"${latest_close:,.2f}",
                        delta=f"{delta_val:,.2f}"
                    )

                    # Candlestick Chart
                    fig = go.Figure(data=[go.Candlestick(
                        x=symbol_df['window_start'],
                        open=symbol_df['open'],
                        high=symbol_df['high'],
                        low=symbol_df['low'],
                        close=symbol_df['close']
                    )])

                    fig.update_layout(
                        title=f"{symbol} 1-Minute Candles",
                        yaxis_title="Price (USD)",
                        xaxis_rangeslider_visible=False,
                        height=400,
                        margin=dict(l=20, r=20, t=40, b=20)
                    )

                    # --- FIX IS HERE: ADD UNIQUE KEY ---
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_{symbol}")

    # Refresh every 2 seconds
    time.sleep(2)