import os
from dotenv import load_dotenv

load_dotenv()

class Constant:
  SOURCE_DATA_LAKE= os.getenv("SOURCE_DATA_LAKE")
  SOURCE_AGENTS_SHEET_ID = os.getenv("SOURCE_AGENTS_SHEET_ID")

  DESTINATION_DATA_LAKE = os.getenv("DESTINATION_DATA_LAKE")

  
