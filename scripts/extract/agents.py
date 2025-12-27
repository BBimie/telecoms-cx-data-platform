import gspread
import pandas as pd
from datetime import datetime
from scripts.common.constant import Constant
import io
from scripts.connection.aws import AWSClient
import logging

logging.basicConfig(level=logging.INFO)

# ## configs ####
SOURCE_AGENTS_SHEET_ID  = Constant.SOURCE_AGENTS_SHEET_ID
DESTINATION_DATA_LAKE = Constant.DESTINATION_DATA_LAKE
DESTINATION_AGENT_KEY = "raw/agents/agents.parquet"
GOOGLE_CREDENTIALS_PATH= Constant.GOOGLE_JSON_PATH
CHUNK_SIZE = Constant.CHUNK_SIZE

def extract_agents_data():
	""" Method to connect to google sheet and fetch agents data."""
	# Error handling in case ID is NonE
	if not SOURCE_AGENTS_SHEET_ID:
		raise ValueError("Agent Sheet ID is missing from .env")

	try:
		#intialize gspread
		logging.info('Initializing gspread client')
		gc_client = gspread.service_account('google_secret.json')

		#open sheet
		logging.info("Open Agents Google sheet")
		workbook = gc_client.open_by_key(SOURCE_AGENTS_SHEET_ID)
		agent_sheet = workbook.worksheet("agents")
		
		#get total number of rows in data
		total_rows = agent_sheet.row_count
		logging.info(f"The Agent sheet has {total_rows} rows (including header).")

		# separate headers from actual data
		headers = agent_sheet.row_values(1)
		if not headers:
			logging.warning("Sheet is empty!")
			#stop reading here
			return
		
		#iterate through the data in chunks
		start_row = 2 
		counter = 1
		
		while start_row <= total_rows:
			end_row = min(start_row + CHUNK_SIZE - 1, total_rows)
			chunk_data = agent_sheet.get_values(f"{start_row}:{end_row}")
			print(start_row, end_row)

			#if the chunk is empty
			if not chunk_data or chunk_data == [[]]:
				logging.info("Hit empty rows, stopping early.")
				break
			
			#save chunk data in df
			chunk_df = pd.DataFrame(chunk_data, columns=headers)
		
			# remove empty rows if any
			chunk_df.dropna(how='all', inplace=True)

			#if the chunk is empty after removing empty rows, skip
			if chunk_df.empty:
				logging.info("Chunk was empty after dropna, skipping")
				start_row += CHUNK_SIZE
				continue

			# Add Metadata
			chunk_df['_data_load_time'] = datetime.now()

			# 4. Upload Chunk to S3
			out_buffer = io.BytesIO()
			chunk_df.to_parquet(out_buffer, index=False)

			#unique file name for this chunk
			s3_key = f"{DESTINATION_DATA_LAKE}/agents_chunk_{counter}.parquet"
			logging.info(f"Uploading chunk {counter} to {s3_key}")

			#push chunk to datalake 
			logging.info(f"Pushing to {DESTINATION_DATA_LAKE}")
			AWSClient().local_s3.put_object(
				Bucket=DESTINATION_DATA_LAKE,
				Key=s3_key,
				Body=out_buffer.getvalue()
			)

			# Prepare for next CHUNK
			start_row += CHUNK_SIZE
			counter += 1

		logging.info(f"Agent data ingested successfully!")
	
	except Exception as e:
		logging.info(f"Could not get agent data, {e}")
		raise RuntimeError(f"Pipeline Halt: Unable to get Agents Data from google sheet, {e}")


if __name__ == "__main__":
    extract_agents_data()