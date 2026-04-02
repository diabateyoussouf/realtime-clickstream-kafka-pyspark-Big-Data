import streamlit as st
import pandas as pd
import plotly.express as px
import glob
import os
import time

st.set_page_config(page_title="Real-Time E-Commerce", layout="wide", initial_sidebar_state="collapsed")

def load_perf_data():
    start_load = time.time()
    path = "./exports_ml/*.csv"
    all_files = glob.glob(path)
    if not all_files: return pd.DataFrame(), 0, 0
    
    all_files.sort(key=os.path.getmtime)
    latest_files = all_files[-100:]
    
    df_list = []
    for f in latest_files:
        try:
            if os.path.getsize(f) > 0:
                temp_df = pd.read_csv(f)
                if not temp_df.empty: df_list.append(temp_df)
        except: continue
        
    final_df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
    return final_df, (time.time() - start_load), len(all_files)

st.title("🛒 E-Commerce Real-Time Intelligence")
st.markdown("---")

main_placeholder = st.empty()

while True:
    df, load_time, total_files = load_perf_data()
    
    with main_placeholder.container():
        if df.empty:
            st.info("📡 En attente des données de Spark...")
        else:
            # Préparation des données
            df['processed_at'] = pd.to_datetime(df['processed_at']).dt.tz_localize(None)
            latency = max(0, (pd.Timestamp.now() - df['processed_at'].max()).total_seconds())

            df['Segment'] = pd.cut(
                df['propensity_score'], 
                bins=[0, 50, 75, 100], 
                labels=['Froid (<50%)', 'Tiède (50-75%)', 'Chaud (>75%)']
            )
            ca_recuperable = df[df['Segment'] == 'Chaud (>75%)']['total_cart_value'].sum()

            # ==========================================
            # 1. KPIs BUSINESS GLOBAUX (Toujours visibles)
            # ==========================================
            b1, b2, b3, b4 = st.columns(4)
            b1.metric("🚨 Abandons Totaux", f"{len(df):,}")
            b2.metric("💸 CA Total en Danger", f"{df['total_cart_value'].sum():,.0f} $")
            b3.metric("🎯 CA Récupérable (Chauds)", f"{ca_recuperable:,.0f} $")
            b4.metric("⏱️ Latence Pipeline", f"{latency:.1f}s")
            
            st.markdown("<br>", unsafe_allow_html=True)

            # ==========================================
            # CRÉATION DES ONGLETS (TABS)
            # ==========================================
            tab1, tab2, tab3 = st.tabs(["📊 Vue Générale", "🐋 VIP & Baleines", "📦 Profil des Paniers"])

            # --- ONGLET 1 : VUE GÉNÉRALE ---
            with tab1:
                col1, col2 = st.columns(2)
                with col1:
                    segment_counts = df['Segment'].value_counts().reset_index()
                    segment_counts.columns = ['Segment', 'Nombre']
                    fig_pie = px.pie(segment_counts, names='Segment', values='Nombre', hole=0.4,
                                     template="plotly_dark", color='Segment',
                                     color_discrete_map={'Chaud (>75%)': '#ff4b4b', 'Tiède (50-75%)': '#ffa500', 'Froid (<50%)': '#00d4ff'})
                    fig_pie.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0))
                    st.plotly_chart(fig_pie, use_container_width=True, key=f"pie_{time.time()}")

                with col2:
                    fig_scatter = px.scatter(df, x='view_count', y='total_cart_value', color='propensity_score',
                                             size='cart_count', hover_data=['user_id'], color_continuous_scale='Inferno',
                                             template="plotly_dark", labels={'view_count': "Vues", 'total_cart_value': "Panier ($)"})
                    fig_scatter.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0))
                    st.plotly_chart(fig_scatter, use_container_width=True, key=f"scatter_{time.time()}")

                st.subheader("🚀 Débit de Traitement (Msg/sec)")
                df['sec'] = df['processed_at'].dt.second
                throughput_df = df.groupby('sec').size().reset_index(name='count')
                fig_perf = px.area(throughput_df, x='sec', y='count', template="plotly_dark", color_discrete_sequence=['#00CC96'])
                fig_perf.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig_perf, use_container_width=True, key=f"c_{time.time()}")

            # --- ONGLET 2 : VIP & BALEINES ---
            with tab2:
                st.markdown("### 🚨 Alerte Clients à Haute Valeur")
                st.write("Ce tableau isole uniquement les clients avec des paniers importants **(> 500$)** et un comportement très engagé.")
                
                # Filtrage des Baleines
                vip_df = df[(df['total_cart_value'] >= 500) & (df['propensity_score'] >= 75)]
                
                if not vip_df.empty:
                    col_v1, col_v2 = st.columns([1, 2])
                    with col_v1:
                        st.metric("Whales Détectées", len(vip_df))
                        st.metric("Valeur VIP Sécurisable", f"{vip_df['total_cart_value'].sum():,.0f} $")
                    with col_v2:
                        st.dataframe(
                            vip_df.sort_values('total_cart_value', ascending=False)[['user_id', 'total_cart_value', 'propensity_score', 'cart_count']],
                            use_container_width=True, hide_index=True, height=250
                        )
                else:
                    st.success("Aucune Baleine en abandon de panier pour le moment.")

            # --- ONGLET 3 : PROFIL DES PANIERS ---
            with tab3:
                st.markdown("### 📦 Distribution des montants abandonnés")
                # Histogramme des prix
                fig_hist = px.histogram(df, x="total_cart_value", nbins=50, 
                                        template="plotly_dark", 
                                        color_discrete_sequence=['#ab63fa'],
                                        labels={'total_cart_value': 'Montant du Panier ($)'})
                fig_hist.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig_hist, use_container_width=True, key=f"hist_{time.time()}")

    time.sleep(2)