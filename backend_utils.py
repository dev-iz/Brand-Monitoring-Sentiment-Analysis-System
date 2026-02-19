import sqlite3  # TO CREATE DATABASE
import ollama  # TO USE OLLAMA MODELS
import praw #PRAW stands for Python Reddit API Wrapper, PRAW is a Python library that makes it easy for you to use the Reddit API in your code.
import pandas as pd # FOR DATA MANIPULATION
from datetime import datetime # FOR DATA MANIPULATION
import streamlit as st # FOR INTERFACE CREATION.

DB_NAME = "brand_monitor.db" # DATABASE MAME
DEFAULT_OLLAMA_MODEL = "mistral:7b" # MODEL OF OLLAMA, WE ARE GOING TO USE



# AS PER OUR FLOW , LETS CREATE AN DATABASE FIRST , FOR THIS PROJECT WE WILL BE CREATING SQL-LITE DATABASE

# FUNCTION TO CREATE AN DATABASE.
def init_db():
    """
    Initializes the database.
    Creates the 'mentions' table (including the 'brand' column)
    if it doesn't exist.
    """
    with sqlite3.connect(DB_NAME) as conn: #This line opens (or creates) a real SQLite database file named brand_monitor.db on your computer’s disk, where all your data will be saved.
        cursor = conn.cursor()  # Create a cursor object to execute SQL commands in the database.
                                # when you are managing SQL using python, we need an agent to interact with the database [cursor]

        # CREATE TABLE statement.
        # creates a table named mentions into your database,if a table with that name does not already exist.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand TEXT NOT NULL,  
                source TEXT NOT NULL,
                text TEXT NOT NULL,
                url TEXT,
                timestamp DATETIME,
                sentiment TEXT,
                topic TEXT,
                urgency TEXT
            )
        """)
        # COLUMN NUMBER 2,3,4,5,6 VALUES NEEDS TO BE INSERTED.
        # COLUMN 7,8,9 , FILLED AFTER ANALYSIS.

    # DATABASE CREATED.



#-----------------------------------------------------------------------------------------------------------------------------
# STEP 2 OF THE FLOW


# DATABASE IS CREATED , NOW I NEED TO INSERT DATA INTO THE DATABASE.
# WHILE INSERTING DATA, I NEED TO MAKE SURE THAT THERE ARE NO DUPLICATES [ SAME MENTION SHOULD NOT BE INSERTED TWICE]
# TO PREVENT DUPLICATE ENTRIES , I WILL RELY ON , URL , AS EVERY POST OR MENTION HAVE AN UNIQUE URL.


# WE WILL CREATE AN FUNCTION THAT WILL CHECK URL COLUMN OF THE TABLE Mentions,  IF ANY PARTICULAR URL EXISTS
# ENTRY WILL NOT BE MADE , ELSE ENTRY WOULD BE MADE.

def add_mention(brand_name, source, text, url, timestamp): # GET VALUES.
    """Adds a new mention with its brand."""
    with sqlite3.connect(DB_NAME) as conn: # CREATING CONNECT TO DATABASE
        cursor = conn.cursor() # CREATING CURSOR TO DATABASE
        cursor.execute("SELECT id FROM mentions WHERE url=?", (url,)) # CHECKING FOR URL
        if not cursor.fetchone(): # IF NO URL FOUND
            cursor.execute(
                "INSERT INTO mentions (brand, source, text, url, timestamp) VALUES (?, ?, ?, ?, ?)", # INSERTING THE DATA
                (brand_name, source, text, url, timestamp)
            )


# WE HAVE NOW FUNCTIONS TO CREATE DATABASE AND ENTER VALUES INTO IT.
# -----------------------------------------------------------------------------------------------------------------------------

#STEP 3 OF THE FLOW


# STEP 4 BEFORE STEP 3 -- FUNCTION TO FETCH ROWS RELATED TO ANY BRAND .
def get_all_mentions_as_df(brand_name):  # Fetches all mentions for a SPECIFIC brand.
    """
    Fetches ALL mentions for a SPECIFIC brand.
    """
    with sqlite3.connect(DB_NAME) as conn:  # CREATING CONNECTION TO DATABASE
        df = pd.read_sql_query(
            "SELECT * FROM mentions WHERE brand = ? ORDER BY timestamp DESC",
            conn,
            params=(brand_name,)
            # FINDING FOR ALL THE ROWS THAT ARE RELATED TO ANY BRAND , AND CREATE AN DATAFRAME OUT OF IT/
        )

        if 'timestamp' in df.columns:  # CONVERT THE TIME-STAMP COLUMN  DATA-TYPE TO DATE-TIME
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
# -----------------------------------------------------------------------------------------------------------------------------




# STEP 3 : FETCH DATA FROM REDDIT API AND INSERT IT INTO THE TABLE CREATED ABOVE USING THE FUNCTION ADD MENTION.

# --- Data Fetching Functions ---

def fetch_reddit_mentions(brand_name, subreddits_list, client_id, client_secret):
    try:
        # creates a Praw reddit instance.
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent="BrandMonitorApp v1.1 by /u/LocalUser",  # Hardcoded user agent
            read_only=True,
        )

        added_count = 0  # Counter to keep track of how many new mentions are added to your DB during this run.
        processed_urls = set()  #A set to remember URLs you already added during this function run [ NEED NOT TO ADD THEM AGAIN]
        existing_df = get_all_mentions_as_df(
            brand_name)  # Reads all existing mentions for this brand from your database into a pandas DataFrame.
        # This is used to avoid inserting duplicates that are already stored.
        existing_urls = set(
            existing_df['url'].tolist())  # The code collects existing URLs into a set to ensure only new,
        # non-duplicate mentions are added.

        for sub_name in subreddits_list:  # iterate over the sub-reddit  list
            try:
                subreddit = reddit.subreddit(
                    sub_name)  # saying PRAW instance to go to subreddit[group] whose name is stored sub_name.
                for post in subreddit.search(query=brand_name, sort="new", limit=20,
                                             time_filter="day"):  # now my praw is inside the sub-reddit
                    # i am asking my sub-reddit to search for most recent 20 posts from the past day that mention the brand name.

                    post_url = f"https://www.reddit.com{post.permalink}"  # Create the full link to that Reddit post.

                    if post_url not in existing_urls and post_url not in processed_urls:  # checking if th url extrated exists already or not.

                        text_to_add = f"{post.title} {post.selftext}"  # Concatenates post title and selftext into one string.
                        # we will store this in text_to_add_column.

                        # calls the add mention function to insert mentions into the DB.
                        if add_mention(brand_name, "Reddit", text_to_add, post_url,
                                       datetime.fromtimestamp(
                                           post.created_utc)):  # converts the post's timestamp (Unix epoch in seconds) into a Python datetime.
                            added_count += 1  # if added something , then increase the count
                            processed_urls.add(
                                post_url)  # add the url into processed url as post of this url have been added.

            except Exception as e:
                st.warning(
                    f"Could not fetch from r/{sub_name}: {e}")  # if anything goes wring handling catch the error.
        return added_count  # After processing all subreddits successfully, return how many new mentions were added in total.

    except Exception as e:  # outside try ka exception.
        # Improved error handling for 401
        if "401" in str(e):
            st.error(f"Reddit API Error: 401 Unauthorized. Please check your Client ID and Client Secret.")
        else:
            st.error(f"Reddit API Error: {e}")
        return 0




# NOW OUR TABLE IS UPDATED WITH NEW ROWS ,
# -----------------------------------------------------------------------------------------------------------------------------
#STEP 5 :- ANALYSIS.



# get sentiment
def get_sentiment(text):# get text as an input [ text will be an row ]
    # Define the instruction prompt
    prompt_template = "Analyze the sentiment of the following text. Is it Positive, Negative, or Neutral? Answer with only one word."

    # Combine the instruction and the text into a single prompt
    full_prompt = f"{prompt_template}\n\nText to analyze:\n{text}"

    try:
        response = ollama.generate(
            model=DEFAULT_OLLAMA_MODEL,
            prompt=full_prompt
        )
        # The response key is 'response' for ollama.generate
        return response["response"].strip()
    except Exception as e:
        st.error(f"Ollama error (Sentiment): {e}")
        return None



#get topic.
def get_topic(text):
    """Classifies the text into topics using ollama.generate."""

    # Define the instruction prompt
    prompt_template = """
    You are a text analysis engine. Your task is to read the following text and assign the **single best-fitting** category from the list below.
    **Categories & Definitions:**
    
    * **Customer Service Issue**: Problems with support, billing, shipping, or account interaction.
    * **Product Defect/Bug**: The product is broken, crashing, or not working as intended.
    * **High Price Complaint**: Feedback that the product or service is too expensive.
    * **Positive Review**: General praise, compliments, or success stories.
    * **Competitor Comparison**: The text explicitly mentions a competitor.
    * **Feature Request**: A suggestion for a new feature or an improvement to an existing one.
    * **PR/News**: Text that appears to be a press release, news article, or public announcement.
    * **Other**: Any other topic that does not clearly fit one of the categories above (e.g., general inquiry, spam, wrong email).
    
    **Rules:**
    1.  Choose only **one** category.
    2.  If none of the specific categories are a good match, you must use **'Other'**.
    3.  Output only the category name.
    """

    # Combine the instruction and the text into a single prompt
    full_prompt = f"{prompt_template}\n\nText to analyze:\n{text}"

    try:
        response = ollama.generate(
            model=DEFAULT_OLLAMA_MODEL,
            prompt=full_prompt
        )
        return response["response"].strip()
    except Exception as e:
        st.error(f"Ollama error (Topic): {e}")
        return None



# get urgency
def get_urgency(text):
    """Determines the urgency of a text using ollama.generate."""

    # Define the instruction prompt
    prompt_template = """
    You are a PR crisis manager. Read this text. Is this a 'High Urgency' issue
    (e.g., safety risk, potential PR crisis, going viral) or a 'Low Urgency'
    issue (e.g., single user complaint, question)? Answer with 'High Urgency' or 'Low Urgency'.
    """

    # Combine the instruction and the text into a single prompt
    full_prompt = f"{prompt_template}\n\nText to analyze:\n{text}"

    try:
        response = ollama.generate(
            model=DEFAULT_OLLAMA_MODEL,
            prompt=full_prompt
        )
        return response["response"].strip()
    except Exception as e:
        st.error(f"Ollama error (Urgency): {e}")
        return None


# IN OUR TABLE , WE HAVE INSERTED FEW COLUMNS  USING THE FUNCTION FETCH DATA ,
# REMAINING VALUES WE HAVE FOUND , LETS INSERT THEM NOW.


# UPDATION FUNCTION
def update_mention_analysis(mention_id, sentiment, topic, urgency):
    """Updates a mention with its AI analysis."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE mentions
            SET sentiment = ?, topic = ?, urgency = ?
            WHERE id = ?
            """,
            (sentiment, topic, urgency, mention_id)
        )
        conn.commit()





def generate_positive_report_summary(df):
    """Generates a detailed, high-level summary of POSITIVE feedback."""
    positive_texts = "\n---\n".join(df[df['sentiment'] == 'Positive']['text'].tolist())
    if not positive_texts:
        return "No positive feedback found to summarize."

    positive_texts_subset = positive_texts[:4000]
    full_prompt = f"""
    You are an expert customer experience analyst.
    Your task is to analyze the following POSITIVE customer feedback and identify the main strengths appreciated by customers.

    Please provide a concise, business-oriented summary in exactly 3 bullet points covering:
    1. The top recurring points or aspects customers praised.
    2. The underlying strengths or reasons behind this positive sentiment (e.g., product quality, service experience, brand trust, etc.).
    3. The potential opportunities for the brand to further capitalize on these strengths.

    Be objective, avoid repetition, and use short, impactful sentences.

    POSITIVE CUSTOMER FEEDBACK:
    {positive_texts_subset}
    """

    try:
        response = ollama.generate(
            model=DEFAULT_OLLAMA_MODEL,
            prompt=full_prompt
        )
        return response["response"].strip()
    except Exception as e:
        st.error(f"Ollama error (Positive Summary): {e}")
        return "Error generating summary."






def generate_negative_report_summary(df):
    """Generates a detailed, high-level summary of NEGATIVE feedback."""
    negative_texts = "\n---\n".join(df[df['sentiment'] == 'Negative']['text'].tolist())
    if not negative_texts:
        return "No negative feedback found to summarize."

    negative_texts_subset = negative_texts[:4000]
    full_prompt = f"""
    You are an expert customer experience analyst.
    Your task is to analyze the following NEGATIVE customer feedback and identify the most common pain points.

    Please provide a concise, business-oriented summary in exactly 3 bullet points covering:
    1. The top recurring complaints or issues customers mentioned.
    2. The underlying cause or pattern behind these issues (if visible).
    3. The potential impact or area of improvement for the brand.

    Be objective, avoid repetition, and use short, impactful sentences.

    NEGATIVE CUSTOMER FEEDBACK:
    {negative_texts_subset}
    """

    try:
        response = ollama.generate(
            model=DEFAULT_OLLAMA_MODEL,
            prompt=full_prompt
        )
        return response["response"].strip()
    except Exception as e:
        st.error(f"Ollama error (Negative Summary): {e}")
        return "Error generating summary."




def generate_report_summary(df):
    """Generates a high-level summary of NEGATIVE feedback using ollama.generate."""
    negative_texts = "\n---\n".join(df[df['sentiment'] == 'Negative']['text'].tolist())
    if not negative_texts:
        return "No negative feedback found to summarize."

    negative_texts_subset = negative_texts[:4000]

    # The prompt already contains the text, so it's the 'full_prompt'
    full_prompt = f"""
        You are a product strategist. Read the following customer suggestions and feature requests.
        Analyze the underlying needs and ideas.

        Based *only* on these comments, provide a bullet-point summary of:
        1.  **Top Suggestions:** What are the most common or impactful ideas users are asking for?
        2.  **Future Opportunities:** What new features or future directions should the company consider working on based on these suggestions?

        Group similar ideas together.

        CUSTOMER SUGGESTIONS:
        {negative_texts_subset}
        """

    try:
        response = ollama.generate(
            model=DEFAULT_OLLAMA_MODEL,
            prompt=full_prompt
        )
        return response["response"].strip()
    except Exception as e:
        st.error(f"Ollama error (Negative Summary): {e}")
        return "Error generating summary."



