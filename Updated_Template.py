import streamlit as st
import sys
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("=== STREAMLIT APP STARTING ===")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"PORT environment variable: {os.environ.get('PORT', 'NOT SET')}")
        logger.info(f"Files in current directory: {os.listdir('.')}")
        
        st.set_page_config(
            page_title="Test App",
            page_icon="🧪",
            layout="wide"
        )
        
        st.title("🧪 Cloud Run Test App")
        st.write("If you can see this, the basic Streamlit setup is working!")
        
        st.write("### System Information:")
        st.write(f"**Current Time:** {datetime.now()}")
        st.write(f"**Python Version:** {sys.version}")
        st.write(f"**Working Directory:** {os.getcwd()}")
        st.write(f"**Environment PORT:** {os.environ.get('PORT', 'Not set')}")
        
        # Test basic functionality
        if st.button("Test Button"):
            st.success("Button clicked successfully!")
            logger.info("Test button clicked")
        
        # Test sidebar
        with st.sidebar:
            st.write("### Sidebar Test")
            option = st.selectbox("Choose an option:", ["Option 1", "Option 2", "Option 3"])
            st.write(f"You selected: {option}")
        
        logger.info("=== STREAMLIT APP LOADED SUCCESSFULLY ===")
        
    except Exception as e:
        error_msg = f"ERROR in main(): {str(e)}"
        logger.error(error_msg)
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception args: {e.args}")
        
        # Also show error in Streamlit
        st.error(f"Application Error: {str(e)}")
        
        # Re-raise to ensure container crashes with error
        raise

if __name__ == "__main__":
    try:
        logger.info("=== CONTAINER STARTUP ===")
        main()
    except Exception as e:
        logger.critical(f"CRITICAL ERROR: {str(e)}")
        import traceback
        logger.critical(f"Full traceback:\n{traceback.format_exc()}")
        sys.exit(1)
