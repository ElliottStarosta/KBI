import os
import pandas as pd
from datetime import datetime, timedelta
import re
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from googleapiclient.discovery import build
import webbrowser
import traceback

import glob
import time

from dotenv import load_dotenv

from logger import Logger
from colorama import Fore, Style

load_dotenv()

CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH', 'csv_files')
TOKEN_FILE = os.getenv('TOKEN_FILE', 'token.json')
CLIENT_SECRETS_FILE = os.getenv('CLIENT_SECRETS_FILE', 'client_secret.json')

# Google API settings
SCOPES = os.getenv('GOOGLE_SCOPES', 'https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive').split(',')
OAUTH_REDIRECT_URI = os.getenv('OAUTH_REDIRECT_URI', 'urn:ietf:wg:oauth:2.0:oob')
LOCAL_SERVER_PORT = int(os.getenv('LOCAL_SERVER_PORT', '8080'))

# File processing settings
MIN_CSV_FILES_REQUIRED = int(os.getenv('MIN_CSV_FILES_REQUIRED', '3'))
MAX_CSV_FILES_TO_PROCESS = int(os.getenv('MAX_CSV_FILES_TO_PROCESS', '3'))

# Spreadsheet formatting settings
FONT_FAMILY = os.getenv('FONT_FAMILY', 'Arial')
FONT_SIZE = int(os.getenv('FONT_SIZE', '12'))
HEADER_BACKGROUND = {
    'red': float(os.getenv('HEADER_BACKGROUND_RED', '0.9')),
    'green': float(os.getenv('HEADER_BACKGROUND_GREEN', '0.9')),
    'blue': float(os.getenv('HEADER_BACKGROUND_BLUE', '0.9'))
}

# Column widths
COLUMN_WIDTHS = {
    0: int(os.getenv('COL_DAY_OF_WEEK_WIDTH', '78')),
    1: int(os.getenv('COL_DATE_WIDTH', '61')),
    2: int(os.getenv('COL_HEBREW_DAY_WIDTH', '68')),
    3: int(os.getenv('COL_HEBREW_MONTH_WIDTH', '73')),
    4: int(os.getenv('COL_DECEASED_FIRST_NAME_WIDTH', '90')),
    5: int(os.getenv('COL_DECEASED_LAST_NAME_WIDTH', '145')),
    6: None,  # Calculated dynamically
    7: int(os.getenv('COL_MOURNER_FIRST_NAME_WIDTH', '90')),
    8: int(os.getenv('COL_MOURNER_LAST_NAME_WIDTH', '100')),
    9: int(os.getenv('COL_RELATIONSHIP_WIDTH', '104')),
    10: int(os.getenv('COL_TRIBE_WIDTH', '60'))
}

HEBREW_NAME_MIN_WIDTH = int(os.getenv('COL_DECEASED_HEBREW_NAME_MIN_WIDTH', '80'))
HEBREW_NAME_PIXELS_PER_CHAR = int(os.getenv('HEBREW_NAME_PIXELS_PER_CHAR', '8'))
HEBREW_NAME_PADDING = int(os.getenv('HEBREW_NAME_PADDING', '30'))



def load_data_from_csv(file_path):
    """Load data from the CSV file and rename columns to match the required format."""
    df = pd.read_csv(file_path)
    
    # Rename columns to match the required header format
    df = df.rename(columns={
        'Yahrzeit Day of the Week': 'Day of the Week',
        'Yahrzeit Long Date': 'Date',
        'Hebrew Day': 'Hebrew Day',
        'Observance Hebrew Month': 'Hebrew Month',
        'Deceased First Name': 'Deceased First Name',
        'Deceased Last Name': 'Deceased Last Name',
        'Deceased Name Hebrew': 'Deceased Hebrew Name',
        'First Name': 'Mourner First Name',
        'Last Name': 'Mourner Last Name',
        'Relationship deceased to mourner': 'Relationship to mourner',
        'Tribe': 'Tribe'
    })
    
    return df

def clean_data(df):
    """Clean data to prevent formatting issues."""
    # Replace NaN values with empty strings
    df = df.fillna('')
    
    # Convert 'Hebrew Day' column to string type to avoid type errors
    if 'Hebrew Day' in df.columns:
        df['Hebrew Day'] = df['Hebrew Day'].astype(str)
    
    # Clean strings - replace special characters, newlines, etc.
    for col in df.columns:
        if df[col].dtype == object:  # Only process string columns
            df[col] = df[col].astype(str).apply(lambda x: x.strip().replace('\n', ' ').replace('\r', ' '))
    
    # Capitalize the relationship values
    if 'Relationship to mourner' in df.columns:
        df['Relationship to mourner'] = df['Relationship to mourner'].apply(
            lambda x: x.capitalize() if isinstance(x, str) and x.lower() != 'none' else x
        )
    
    if 'Day of the Week' in df.columns:
        df['Day of the Week'] = df['Day of the Week'].str[:3]
    
    return df

def parse_date(date_str):
    """Convert date string to datetime object."""
    date_formats = [
        '%d-%b-%y',     # Example: 01-Jun-25
        '%b %d, %Y',    # Example: May 11, 2025
        '%Y-%m-%d',     # Example: 2025-05-11
        '%B %d, %Y'     # Example: July 2, 2025
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def format_date(date_obj):
    """Format datetime object to DD-MMM format."""
    if date_obj is None:
        return ""
    return date_obj.strftime('%d-%b')

def identify_complete_weeks_for_month(target_month, target_year):
    """
    Identify all complete weeks (Saturday to Friday) that include days from the target month.
    Returns a list of (start_date, end_date) tuples for each complete week.
    """
    # Create a datetime for the first day of the target month
    first_day_of_month = datetime(target_year, target_month, 1)
    
    # Calculate the last day of the target month
    if target_month == 12:
        last_day_of_month = datetime(target_year+1, 1, 1) - timedelta(days=1)
    else:
        last_day_of_month = datetime(target_year, target_month+1, 1) - timedelta(days=1)
    
    # Find the Saturday before or on the first day of the month
    days_to_prev_saturday = (first_day_of_month.weekday() - 5) % 7
    first_saturday = first_day_of_month - timedelta(days=days_to_prev_saturday)
    
    # If the first day is not a Saturday, we need to go back to the previous Saturday
    if first_saturday > first_day_of_month:
        first_saturday = first_saturday - timedelta(days=7)
    
    # Find the Friday after or on the last day of the month
    days_to_next_friday = (4 - last_day_of_month.weekday()) % 7
    last_friday = last_day_of_month + timedelta(days=days_to_next_friday)
    
    # Generate all Saturday-Friday weeks
    weeks = []
    current_saturday = first_saturday
    
    while current_saturday <= last_friday:
        current_friday = current_saturday + timedelta(days=6)
        weeks.append((current_saturday, current_friday))
        current_saturday = current_saturday + timedelta(days=7)
    
    return weeks

def extract_month_info_from_filename(file_path):
    """
    Extract month and year information from a file name.
    Returns a tuple of (month_number, year).
    """
    filename = os.path.basename(file_path).lower()
    
    # List of month names for matching
    month_names = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    
    # Try to find a year in the filename (assuming 4-digit year like 2025)
    year_match = re.search(r'20\d{2}', filename)
    year = int(year_match.group(0)) if year_match else None
    
    # Try to find a month name in the filename
    month = None
    for month_name, month_num in month_names.items():
        if month_name in filename:
            month = month_num
            break
    
    # If no month found, try to extract from date format in the file
    if month is None:
        # Try to load the first row to get a date
        try:
            df = pd.read_csv(file_path, nrows=1)
            if 'Yahrzeit Long Date' in df.columns and not df['Yahrzeit Long Date'].empty:
                date_str = df['Yahrzeit Long Date'].iloc[0]
                date_obj = parse_date(date_str)
                if date_obj:
                    month = date_obj.month
        except Exception:
            # If that fails, default to current month
            month = datetime.now().month
            
     # If no month found, try to extract from date format in the file
    if year is None:
        # Try to load the first row to get a date
        try:
            df = pd.read_csv(file_path, nrows=1)
            if 'Yahrzeit Long Date' in df.columns and not df['Yahrzeit Long Date'].empty:
                date_str = df['Yahrzeit Long Date'].iloc[0]
                date_obj = parse_date(date_str)
                if date_obj:
                    year = date_obj.year
        except Exception:
            # If that fails, default to current year
            year = datetime.now().year

    return month, year


def find_middle_month_from_files(file_paths):
    """
    Determine the middle month from the provided file paths.
    Returns a tuple of (middle_month_number, year).
    """
    # Extract month and year from each file
    month_info = []
    for file_path in file_paths:
        month, year = extract_month_info_from_filename(file_path)
        month_info.append((month, year))
    
    # Sort by year and month
    month_info.sort(key=lambda x: (x[1], x[0]))
    
    # Return the middle entry
    middle_index = len(month_info) // 2
    return month_info[middle_index]



def create_weekly_dataframes(df, target_month, target_year):
    """
    Create separate dataframes for each complete week and a master dataframe.
    Returns a tuple of (weekly_dataframes, sheet_names, master_df, title)
    """
    # Make a copy of the original dataframe
    df_copy = df.copy()
    
    # Convert date strings to datetime objects for sorting and filtering
    date_objects = []
    for date_str in df_copy['Date']:
        date_objects.append(parse_date(date_str))
    
    df_copy['_date_obj'] = date_objects
    
    # Sort by date, deceased last name, deceased first name
    df_sorted = df_copy.sort_values(
        by=['_date_obj', 'Deceased Last Name', 'Deceased First Name'],
        na_position='last'
    )
    
    # Identify complete weeks for the target month
    weeks = identify_complete_weeks_for_month(target_month, target_year)
    
    if not weeks:
        Logger.error("No valid weeks found for the target month")
        df_sorted = df_sorted.drop(columns=['_date_obj'])
        return [df_sorted], ["Complete List"], df_sorted, f"Yahrzeit List - No valid weeks"
    
    # Get month name for title
    month_name = datetime(target_year, target_month, 1).strftime('%B')
    spreadsheet_title = f"{target_month} {month_name} {target_year}"
    
    # Create weekly dataframes
    weekly_dfs = []
    sheet_names = []
    
    for start_date, end_date in weeks:
        # Filter data for this week (including data from adjacent months)
        weekly_data = df_sorted[
            (df_sorted['_date_obj'] >= start_date) & 
            (df_sorted['_date_obj'] <= end_date)
        ].copy()
        
        # Only include the week if it contains at least one day from the target month
        month_data = weekly_data[
            (weekly_data['_date_obj'].dt.month == target_month) & 
            (weekly_data['_date_obj'].dt.year == target_year)
        ]
        
        if not month_data.empty:  # Only include weeks that have data from the target month
            # Format dates back to string format
            weekly_data['Date'] = weekly_data['_date_obj'].apply(format_date)
            
            # Apply grouping logic
            weekly_data = group_data_by_date_and_name(weekly_data)
            
            # Drop the datetime column
            weekly_data = weekly_data.drop(columns=['_date_obj'])
            
            # Create sheet name
            sheet_name = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}"
            
            weekly_dfs.append(weekly_data)
            sheet_names.append(sheet_name)
    
    # Now create the master sheet by combining all weekly sheets in order
    if weekly_dfs:
        master_df = pd.concat(weekly_dfs, ignore_index=True)
    else:
        # If no weekly dataframes were created (should not happen given the logic)
        df_sorted['Date'] = df_sorted['_date_obj'].apply(format_date)
        master_df = df_sorted.drop(columns=['_date_obj'])
    
    return weekly_dfs, sheet_names, master_df, spreadsheet_title
# def group_data_by_date_and_name(df):
#     """
#     Group data by date, and within each date group by deceased name.
#     Blanks out repeated fields according to the rules.
#     """
#     # Create a copy of the dataframe
#     result_df = df.copy()
    
#     # Add grouping columns
#     result_df['date_group'] = (result_df['Date'] != result_df['Date'].shift()).cumsum()
    
#     # Process each date group
#     for group_id in result_df['date_group'].unique():
#         group_mask = result_df['date_group'] == group_id
#         group_indices = result_df.index[group_mask].tolist()
        
#         # Blank out date fields for all but the first row in each date group
#         if len(group_indices) > 1:
#             for idx in group_indices[1:]:
#                 result_df.at[idx, 'Day of the Week'] = ''
#                 result_df.at[idx, 'Date'] = ''
#                 result_df.at[idx, 'Hebrew Day'] = ''
#                 result_df.at[idx, 'Hebrew Month'] = ''
                
        
#         # Create a sub-grouping for deceased names within this date group
#         sub_df = result_df[group_mask].copy()
#         sub_df['deceased_name'] = sub_df['Deceased Last Name'] + '|' + sub_df['Deceased First Name']
#         sub_df['name_group'] = (sub_df['deceased_name'] != sub_df['deceased_name'].shift()).cumsum()
        
#         # Process each name group within the date group
#         for name_group_id in sub_df['name_group'].unique():
#             name_mask = sub_df['name_group'] == name_group_id
#             name_indices = sub_df.index[name_mask].tolist()
            
#             # Blank out name fields for all but the first row in each name group
#             if len(name_indices) > 1:
#                 for idx in name_indices[1:]:
#                     result_df.at[idx, 'Deceased First Name'] = ''
#                     result_df.at[idx, 'Deceased Last Name'] = ''
#                     result_df.at[idx, 'Deceased Hebrew Name'] = ''
    
#     # Remove helper columns
#     result_df = result_df.drop(columns=['date_group'])
    
#     return result_df

def group_data_by_date_and_name(df):
    """
    Group data hierarchically by each column, blanking out repeated values
    to create a clean cascading visual effect.
    """
    # Create a copy of the dataframe
    result_df = df.copy()
    
    # Define the hierarchical grouping columns in order
    grouping_columns = [
        'Day of the Week',
        'Date', 
        'Hebrew Day',
        'Hebrew Month',
        'Deceased First Name',
        'Deceased Last Name', 
        'Deceased Hebrew Name',
        'Mourner First Name',
        'Mourner Last Name',
        'Relationship to mourner',
        'Tribe'
    ]
    
    # Only process columns that actually exist in the dataframe
    existing_columns = [col for col in grouping_columns if col in result_df.columns]
    
    # Create grouping keys for each level
    for i, col in enumerate(existing_columns):
        if i == 0:
            # First column - group by itself
            result_df[f'group_{i}'] = (result_df[col] != result_df[col].shift()).cumsum()
        else:
            # Subsequent columns - group by combination of all previous columns plus current
            combined_key = result_df[existing_columns[:i+1]].astype(str).agg('|'.join, axis=1)
            result_df[f'group_{i}'] = (combined_key != combined_key.shift()).cumsum()
    
    # Apply blanking logic for each grouping level
    for i, col in enumerate(existing_columns):
        group_col = f'group_{i}'
        
        # Process each group
        for group_id in result_df[group_col].unique():
            group_mask = result_df[group_col] == group_id
            group_indices = result_df.index[group_mask].tolist()
            
            # Blank out this column for all but the first occurrence in the group
            if len(group_indices) > 1:
                for idx in group_indices[1:]:
                    result_df.at[idx, col] = ''
    
    # Remove helper columns
    helper_columns = [col for col in result_df.columns if col.startswith('group_')]
    result_df = result_df.drop(columns=helper_columns)
    
    return result_df


def connect_to_google_sheets():
    """Authenticate and connect to Google Sheets API using OAuth2."""
    try:
        
        creds = None
        
        # Check if we have valid stored credentials
        if os.path.exists(TOKEN_FILE):
            try:
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            except Exception as e:
                Logger.warning(f"Invalid token file: {e}")
                # Delete the corrupted token file
                os.remove(TOKEN_FILE)
                Logger.info("Deleted corrupted token file. Will re-authenticate.")
                creds = None
        
        # If no valid credentials, request authentication
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    Logger.warning(f"Token refresh failed: {e}")
                    # Delete the token file and re-authenticate
                    if os.path.exists(TOKEN_FILE):
                        os.remove(TOKEN_FILE)
                    creds = None
            
            if not creds:
                if not os.path.exists(CLIENT_SECRETS_FILE):
                    Logger.error(f"{CLIENT_SECRETS_FILE} not found.")
                    Logger.info("Please download your OAuth credentials from Google Cloud Console")
                    Logger.info("and save them as 'client_secret.json' in this directory.")
                    return None, None
                
                # Try local server authentication
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
                    creds = flow.run_local_server(port=LOCAL_SERVER_PORT)
                except Exception as local_error:
                    Logger.error(f"Local server authentication failed: {local_error}")
                    Logger.info("Trying manual authentication instead...")
                    
                    # Fall back to manual authentication
                    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
                    flow.redirect_uri = OAUTH_REDIRECT_URI
                    
                    auth_url, _ = flow.authorization_url(access_type='offline', prompt='consent', include_granted_scopes='true')
                    Logger.info("\n" + "="*80)
                    Logger.info("Please open this URL in your browser to authenticate:")
                    Logger.info(auth_url)
                    Logger.info("="*80 + "\n")
                    
                    code = input("Enter the authorization code shown in the browser: ")
                    flow.fetch_token(code=code)
                    creds = flow.credentials
            
            # Save the credentials for future runs
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
        
        # Create the services
        sheets_service = build('sheets', 'v4', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)
        
        return sheets_service, drive_service
    
    except Exception as e:
        Logger.error(f"Error connecting to Google Sheets API: {str(e)}")
        return None, None
    
def create_spreadsheet(drive_service, title="Yahrzeit List"):
    """Create a new Google Sheet and return its ID."""
    try:
        file_metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        
        file = drive_service.files().create(body=file_metadata, fields='id').execute()
        spreadsheet_id = file.get('id')
        
        return spreadsheet_id
    
    except Exception as e:
        Logger.error(f"Error creating spreadsheet: {e}")
        return None

def format_sheet(service, spreadsheet_id, sheet_id):
    """Apply formatting to a sheet with Arial 12 bold font and grid lines."""
    try:
        requests = []
        
        # Format header row - bold, center, gray background, and wrap text
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': 11  # A–K (inclusive)
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'CENTER',
                        'textFormat': {
                            'fontFamily': FONT_FAMILY,
                            'fontSize': FONT_SIZE,
                            'bold': True
                        },
                        'backgroundColor': HEADER_BACKGROUND,
                        'wrapStrategy': 'WRAP'
                    }
                },
                'fields': 'userEnteredFormat(horizontalAlignment,textFormat,backgroundColor,wrapStrategy)'
            }
        })

        # Apply Arial font size 12 bold to columns A-C (0-2) with center alignment
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,  # Start after header
                    'startColumnIndex': 0,
                    'endColumnIndex': 3
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'CENTER',
                        'verticalAlignment': 'MIDDLE',
                        'textFormat': {
                            'fontFamily': FONT_FAMILY,
                            'fontSize': FONT_SIZE,
                            'bold': True
                        },
                        'wrapStrategy': 'OVERFLOW_CELL'
                    }
                },
                'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat,wrapStrategy)'
            }
        })
        
        # Apply Arial font size 12 bold to columns D-K (3-10) with left alignment
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,  # Start after header
                    'startColumnIndex': 3,
                    'endColumnIndex': 11
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'LEFT',
                        'verticalAlignment': 'MIDDLE',
                        'textFormat': {
                            'fontFamily': 'Arial',
                            'fontSize': 12,
                            'bold': True
                        },
                        'wrapStrategy': 'OVERFLOW_CELL'
                    }
                },
                'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat,wrapStrategy)'
            }
        })
        
        # Freeze the header row
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': 1
                    }
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        })
        
        # Execute the formatting requests
        body = {'requests': requests}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        
    except Exception as e:
        Logger.error(f"Error formatting sheet: {e}")

def add_borders_and_resize_columns(service, spreadsheet_id, sheet_id, values):
    """Add borders to all cells with content and resize columns to specific sizes."""
    try:
        if not values or len(values) == 0:
            return

        # Find the maximum row and column with content
        max_row = len(values)
        max_col = max(len(row) for row in values)
        
        # Define column widths based on requirements
        column_widths = COLUMN_WIDTHS.copy()

        # Calculate width for "Deceased Hebrew Name" column (index 6) based on content
        if 6 < max_col:
            max_content_length = 0
            
        
            for row in values:
                if len(row) > 6 and row[6]:
                    content_length = len(str(row[6]))
                    max_content_length = max(max_content_length, content_length)
            # Set minimum width of 80 pixels, with 8 pixels per character + padding
            column_widths[6] = max(HEBREW_NAME_MIN_WIDTH, max_content_length * HEBREW_NAME_PIXELS_PER_CHAR + HEBREW_NAME_PADDING)

        # Add borders to the entire content area
        requests = [{
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': max_row,
                    'startColumnIndex': 0,
                    'endColumnIndex': max_col
                },
                'top': {'style': 'SOLID', 'width': 1},
                'bottom': {'style': 'SOLID', 'width': 1},
                'left': {'style': 'SOLID', 'width': 1},
                'right': {'style': 'SOLID', 'width': 1},
                'innerHorizontal': {'style': 'SOLID', 'width': 1},
                'innerVertical': {'style': 'SOLID', 'width': 1}
            }
        }]
        
        # Add column width adjustment requests
        for col_idx in range(min(max_col, 11)):  # Only process up to column K (index 10)
            if col_idx in column_widths and column_widths[col_idx] is not None:
                pixel_width = column_widths[col_idx]
                
                requests.append({
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': col_idx,
                            'endIndex': col_idx + 1
                        },
                        'properties': {
                            'pixelSize': pixel_width
                        },
                        'fields': 'pixelSize'
                    }
                })
        
        # Execute the formatting requests
        body = {'requests': requests}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        
    except Exception as e:
        Logger.error(f"Error adding borders and resizing columns: {e}")

def prepare_data_for_sheets(df):
    """Convert DataFrame to a format suitable for Google Sheets."""
    # Replace NaN with empty strings
    df_cleaned = df.fillna('')
    
    # Convert DataFrame to list of lists
    header = df_cleaned.columns.tolist()
    values = [header]
    
    # Add rows
    for _, row in df_cleaned.iterrows():
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append('')
            elif isinstance(val, (int, float)):
                row_values.append(str(val))
            else:
                row_values.append(str(val).strip())
        values.append(row_values)
    
    return values

   
def create_and_populate_sheets(service, drive_service, master_df, weekly_sheets, sheet_names, spreadsheet_title):
    """Create a new spreadsheet, populate it with data and apply formatting."""
    try:
        # Create a new spreadsheet with progress
        with Logger.timed_step(f"Creating spreadsheet '{spreadsheet_title}'"):
            spreadsheet_id = create_spreadsheet(drive_service, spreadsheet_title)
            if not spreadsheet_id:
                Logger.error("Failed to create spreadsheet")
                return

        # Get the default sheet ID
        with Logger.timed_step("Initializing spreadsheet structure"):
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            default_sheet_id = spreadsheet['sheets'][0]['properties']['sheetId']

        # Prepare batch update requests
        requests = [{
            'updateSheetProperties': {
                'properties': {
                    'sheetId': default_sheet_id,
                    'title': 'Master'
                },
                'fields': 'title'
            }
        }]

        # Add requests for weekly sheets
        for name in sheet_names:
            requests.append({
                'addSheet': {
                    'properties': {
                        'title': name
                    }
                }
            })

        # Execute batch update to create sheets
        with Logger.timed_step("Creating weekly sheets"):
            body = {'requests': requests}
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

        # Get sheet IDs
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_properties = {sheet['properties']['title']: sheet['properties']['sheetId'] 
                          for sheet in spreadsheet['sheets']}

        # Populate Master sheet with progress
        with Logger.progress(range(4), desc="Populating Master sheet") as pbar:
            master_values = prepare_data_for_sheets(master_df)
            pbar.update(1)  # Data prepared
            
            body = {'values': master_values}
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='Master!A1',
                valueInputOption='RAW',
                body=body
            ).execute()
            pbar.update(1)  # Data uploaded
            
            format_sheet(service, spreadsheet_id, sheet_properties['Master'])
            pbar.update(1)  # Formatting applied
            
            add_borders_and_resize_columns(service, spreadsheet_id, sheet_properties['Master'], master_values)
            pbar.update(1)  # Borders added

        # Populate and format weekly sheets with progress
        total_sheets = len(weekly_sheets)
        for idx, (weekly_df, sheet_name) in enumerate(zip(weekly_sheets, sheet_names), 1):
            with Logger.progress(range(5), desc=f"Processing sheet {idx}/{total_sheets}") as pbar:  # Changed from 4 to 5
                weekly_values = prepare_data_for_sheets(weekly_df)
                pbar.update(1)
                
                body = {'values': weekly_values}
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{sheet_name}'!A1",
                    valueInputOption='RAW',
                    body=body
                ).execute()
                pbar.update(1)
                
                format_sheet(service, spreadsheet_id, sheet_properties[sheet_name])
                pbar.update(1)
                
                add_borders_and_resize_columns(service, spreadsheet_id, sheet_properties[sheet_name], weekly_values)
                pbar.update(1)
                

        Logger.success(f"Successfully created spreadsheet: {spreadsheet_title}")
        Logger.success(f"Access your spreadsheet at: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        
        # Open the URL in the default browser
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        webbrowser.open(url)

    except Exception as e:
        Logger.error(f"Error creating and populating sheets: {e}")
        traceback.print_exc()

def load_and_combine_data(file_paths):
    """Load and combine data from the provided CSV files."""
    if not file_paths:
        return pd.DataFrame()
    
    dataframes = []
    for file in file_paths:
        df = load_data_from_csv(file)
        dataframes.append(df)
    
    # Combine all dataframes
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return clean_data(combined_df)
    
    return pd.DataFrame()


def main():
    Logger.header("Yahrzeit List Processor")
    
    # Hardcoded path to the folder containing CSV files
    folder_path = CSV_FOLDER_PATH
    
    # Check if the folder exists
    if not os.path.isdir(folder_path):
        Logger.error(f"Folder '{folder_path}' not found or is not a directory.")
        return
    
    # Find all CSV files in the folder
    Logger.info("Scanning for CSV files...")
    file_paths = glob.glob(os.path.join(folder_path, "*.csv"))
    
    # Check if at least three CSV files are found
    if len(file_paths) < MIN_CSV_FILES_REQUIRED:
        Logger.error(f"Found only {len(file_paths)} CSV files in '{folder_path}'. At least three CSV files are required.")
        return
    
    # If more than three files found, use the three most recent by modification time
    if len(file_paths) > MIN_CSV_FILES_REQUIRED:
        Logger.warning(f"Found {len(file_paths)} CSV files - using the three most recent")
        file_paths.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        file_paths = file_paths[:3]
        Logger.info(f"Selected files: {', '.join([os.path.basename(f) for f in file_paths])}")
    
    # Find the middle month
    Logger.info("Determining the middle month from the provided files...")
    with Logger.progress(range(3), desc="Analyzing files") as pbar:
        target_month, target_year = find_middle_month_from_files(file_paths)
        for _ in pbar:
            time.sleep(0.1)  # Simulate processing
    
    # Get the month name for display
    target_month_name = datetime(target_year, target_month, 1).strftime('%B')
    Logger.success(f"Middle month determined: {Fore.YELLOW}{target_month_name} {target_year}{Style.RESET_ALL}")
    
    # Load and process data
    Logger.info("Loading and combining CSV data...")
    df = load_and_combine_data(file_paths)
    
    if df.empty:
        Logger.error("No data found in the CSV files.")
        return
    
    Logger.success(f"Data loaded successfully: {Fore.YELLOW}{len(df)} records{Style.RESET_ALL}")
    
    # Create weekly dataframes
    Logger.info("Identifying complete weeks and creating sheet data...")
    weekly_sheets, sheet_names, master_df, spreadsheet_title = create_weekly_dataframes(df, target_month, target_year)
    
    # Connect to Google Sheets API
    Logger.header("Google Sheets Authentication")
    Logger.info("Authenticating with Google Sheets API...")
    Logger.warning("A browser window may open for authentication if needed.")
    
    service, drive_service = connect_to_google_sheets()
    if not service or not drive_service:
        Logger.error("Failed to connect to Google Sheets API")
        return
    
    # Create and format spreadsheet
    Logger.header("Creating Google Sheet")
    Logger.info(f"Creating spreadsheet: {Fore.YELLOW}{spreadsheet_title}{Style.RESET_ALL}")
    create_and_populate_sheets(service, drive_service, master_df, weekly_sheets, sheet_names, spreadsheet_title)
    
    Logger.header("Process Complete")
    Logger.success("Script completed successfully!")


if __name__ == "__main__":
    main()
