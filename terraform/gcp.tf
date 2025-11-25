# Enable APIs (Sheets & Drive)
resource "google_project_service" "sheets_api" {
  service = "sheets.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "drive_api" {
  service = "drive.googleapis.com"
  disable_on_destroy = false
}

# Create the Service Account (The "Robot")
resource "google_service_account" "extractor_sa" {
  account_id   = "python-extractor"
  display_name = "Python Extractor Service Account"
  depends_on   = [google_project_service.sheets_api]
}

# Generate the JSON Key
resource "google_service_account_key" "extractor_key" {
  service_account_id = google_service_account.extractor_sa.name
}

# SAVE KEY TO FILE
# This saves the key directly to your project root as 'google_secret.json'
resource "local_file" "google_key_file" {
  content  = base64decode(google_service_account_key.extractor_key.private_key)
  filename = "${path.module}/../google_secret.json" 
}