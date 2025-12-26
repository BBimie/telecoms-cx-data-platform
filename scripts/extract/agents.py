import gspread
import pandas as pd
from datetime import datetime
from scripts.common.constant import Constant
import io
from scripts.connection.aws import AWSClient
import logging


# ## configs ####
SOURCE_AGENTS_SHEET_ID  = Constant.SOURCE_AGENTS_SHEET_ID
DESTINATION_DATA_LAKE = Constant.DESTINATION_DATA_LAKE
DESTINATION_AGENT_KEY = "raw/agents/agents.parquet"
GOOGLE_CREDENTIALS_PATH= Constant.GOOGLE_JSON_PATH

def extract_agents_data():
	""" Method to connect to google sheet and fetch agents data."""
	# Error handling in case ID is NonE
	if not SOURCE_AGENTS_SHEET_ID:
		raise ValueError("Agent Sheet ID is missing from .env")

	try:
		#intialize gspread
		logging.info('Initializing gspread client')
		gc_client = gspread.service_account(GOOGLE_CREDENTIALS_PATH)

		#open sheet
		logging.info("Open Agents Google sheet")
		workbook = gc_client.open_by_key(SOURCE_AGENTS_SHEET_ID)
		agent_sheet = workbook.worksheet("agents")

		#reading agents data
		agents_df = pd.DataFrame(agent_sheet.get_all_records())

		#adding load time metadata tracking 
		agents_df['_data_load_time'] = datetime.now()
		
		logging.info(f"Agent data columns: {agents_df.columns}")
		logging.info(f"Agent data shape: {agents_df.shape}")
		logging.info(f"Agent data head: {agents_df.head()}")

	
		# save dataframe to parquet
		out_buffer = io.BytesIO()
		agents_df.to_parquet(out_buffer, index=False)


		#pushing to datalake 
		logging.info(f"Pushing to {DESTINATION_DATA_LAKE}")
		AWSClient().local_s3.put_object(
			Bucket=DESTINATION_DATA_LAKE,
			Key=DESTINATION_AGENT_KEY,
			Body=out_buffer.getvalue()
		)

		logging.info(f"Agent data ingested successfully!")
	
	except Exception as e:
		logging.info(f"Could not get agent data, {e}")
		raise RuntimeError(f"Pipeline Halt: Unable to get Agents Data from google sheet, {e}")


if __name__ == "__main__":
    # YOU MUST CALL THE FUNCTION HERE
    extract_agents_data()