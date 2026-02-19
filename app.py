import streamlit as st # for creating interface
import backend_utils as bu # supporting libraries
import plotly.express as px # for building visuals

# --- Page Configuration ---
st.set_page_config(
    page_title="AI Reddit Brand Monitor",
    page_icon="🤖",
    layout="wide"
)

# --- Initialize Database ---
bu.init_db()   # creating/calling database at the start of the program




# --- Streamlit Session State ---
if 'reddit_configured' not in st.session_state:
    st.session_state['reddit_configured'] = False   # using this variable i will see if reddit is configured for the session or not , currently , it is not configured.
if 'brand_name' not in st.session_state:
    st.session_state['brand_name'] = "OpenAI"  # Default brand

# --- Sidebar (Configuration & Actions) ---
with st.sidebar:
    st.title("🤖 AI Reddit Monitor")

    # --- API Key Input Form ---
    if not st.session_state['reddit_configured']: # if no reddit configuration in the session.
        st.header("Reddit API Setup")
        st.info("Please enter your Reddit API credentials to proceed.")

        client_id = st.text_input("REDDIT_CLIENT_ID")
        client_secret = st.text_input("REDDIT_CLIENT_SECRET", type="password")

        if st.button("Save & Connect"):
            if not client_id or not client_secret:
                st.warning("Please fill in both Client ID and Client Secret.")
            else:
                # Save credentials to session state
                st.session_state['reddit_client_id'] = client_id
                st.session_state['reddit_client_secret'] = client_secret
                st.session_state['reddit_configured'] = True
                st.success("Credentials saved!")
                st.rerun()

    # --- Main App Sidebar (if configured) ---
    if st.session_state['reddit_configured']:
        st.info(f"Ollama (`{bu.DEFAULT_OLLAMA_MODEL}`) must be downloaded locally.")

        # --- Configuration ---
        st.header("Configuration")

        # Use session state to persist brand name
        st.session_state.brand_name = st.text_input(
            "Brand/Keyword to Monitor",
            st.session_state.brand_name
        )

        subreddits_str = st.text_area(
            "Subreddits (comma-separated)",
            "OpenAI, ChatGPT, artificial, singularity"
        )
        subreddits_list = [s.strip() for s in subreddits_str.split(",") if s.strip()] # cleaning sub-reddits


        # --- Actions ---
        if st.button("Fetch New Reddit Mentions"): # button
            with st.spinner(f"Fetching Reddit data for '{st.session_state.brand_name}'..."):
                reddit_count = bu.fetch_reddit_mentions(   # calling function
                    st.session_state.brand_name,
                    subreddits_list,
                    st.session_state['reddit_client_id'],  # reddit credentials for authentication.
                    st.session_state['reddit_client_secret']
                ) # it will return the mentioned and inside the function database will have data into it.
                st.success(f"Added {reddit_count} new Reddit posts.")
                st.rerun()  # Rerun to load new data

        #---------ANALYSIS-----------------

        # GETTING DATA FOR THE PARTICULAR BRAND
        all_data_df = bu.get_all_mentions_as_df(st.session_state.brand_name)

        # FROM THE BRAND DATA GETTING THE  DATA THAT NEEDS TO BE ANALYZED.
        pending_df = all_data_df[all_data_df['sentiment'].isnull()]
        st.info(f"**{len(pending_df)}** New mentions")

        # Only show the "Analyze" button if there are pending items
        if not pending_df.empty:
            if st.button(f"Analyze {len(pending_df)} Pending Items"):
                progress_bar = st.progress(0, text="Analyzing mentions...")
                total = len(pending_df)

                for i, row in enumerate(pending_df.itertuples()):  # ITERATING OVER EACH ROW.
                    text_to_analyze = row.text # GETTING TEXT OF ROW

                    # Run analysis
                    sentiment = bu.get_sentiment(text_to_analyze)
                    topic = bu.get_topic(text_to_analyze)
                    urgency = bu.get_urgency(text_to_analyze)

                    if sentiment and topic and urgency:
                        # Update the database
                        bu.update_mention_analysis(row.id, sentiment, topic, urgency)

                    progress_bar.progress((i + 1) / total, text=f"Analyzing item {i + 1}/{total}")

                progress_bar.empty()
                st.success("Analysis complete!")
                st.rerun()  # Rerun to refresh dashboard

#---------------------------------------------------------------

# SHOW VISUAL NOW.

# --- Main Page Dashboard ---
if st.session_state.get('reddit_configured', False): #it checks whether reddit_configured is True
    # Use the brand name from session state
    st.title(f"Reddit Reputation Dashboard: {st.session_state.brand_name}")


    # Filter for analyzed data
    analyzed_df = all_data_df.dropna(subset=['sentiment']).copy()   # remove any rows where sentiment analysis is not done.


    # --- Tabs ---
    tab1, tab2 = st.tabs([
        "Main Dashboard",
        "Raw Data"
    ])

    # --- Tab 1: Main Dashboard ---
    with tab1:
        st.header("Overall Brand Sentiment")

        # --- Pie Chart (First) ---
        sentiment_counts = analyzed_df['sentiment'].value_counts()
        if not sentiment_counts.empty:
            fig_pie = px.pie(
                sentiment_counts,
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="Sentiment Breakdown",
                color=sentiment_counts.index,
                color_discrete_map={'Negative': 'red', 'Positive': 'green', 'Neutral': 'blue'}
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.write("No sentiment data available.")

        # --- Bar Chart (Second) ---

        topics_exploded = analyzed_df['topic'].str.split(',').explode().str.strip()
        topic_counts = topics_exploded.value_counts()

        if not topic_counts.empty:
            fig_bar = px.bar(
                topic_counts,
                x=topic_counts.index,
                y=topic_counts.values,
                title="Top 10 Topics & Problems",
                labels={'x': 'Topic', 'y': 'Count'}
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.write("No topics found.")


        st.divider()
        st.header("Automated Summaries")
        # --- 3 COLUMNS for buttons ---
        col_pos, col_neg, col_sug = st.columns(3)
        with col_pos:
            if st.button("Generate Positive Feedback Summary"):
                with st.spinner("Ollama is summarizing positive feedback..."):
                    summary = bu.generate_positive_report_summary(analyzed_df) #calling the function
                    st.markdown("### Positive Summary:")
                    st.markdown(summary)

        with col_neg:
            if st.button("Generate Negative Feedback Summary"):
                with st.spinner("Ollama is summarizing negative feedback..."):
                    summary = bu.generate_negative_report_summary(analyzed_df) #calling the function
                    st.markdown("### Negative Summary:")
                    st.markdown(summary)

        with col_sug:
            if st.button("Generate Suggestion Summary"):
                with st.spinner("Ollama is summarizing suggestions..."):
                    summary = bu.generate_report_summary(analyzed_df) #calling the function
                    st.markdown("### Suggestion Summary:")
                    st.markdown(summary)

        # --- Tab 2: Raw Data ---
        with tab2:
            st.header(f"All Raw Data for '{st.session_state.brand_name}' from Reddit")
            # Show all data (analyzed and pending)
            st.dataframe(all_data_df, use_container_width=True, hide_index=True)

else:
    # Show this on the main page if not configured
    st.title("Welcome to the AI Reddit Monitor")
    st.info("Please enter your Reddit API credentials in the sidebar to get started.")

