import os
import pandas as pd
import gspread
from datetime import datetime, timedelta
import re
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import numpy as np
import json

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
        'First Name': 'Mourner First Name',
        'Last Name': 'Mourner Last Name',
        'Hebrew Name': 'Hebrew Name',
        'Relationship deceased to mourner': 'Relationship to mourner',
        'Tribe': 'Tribe'
    })
    
    return df

def clean_data(df):
    """Clean data to prevent formatting issues."""
    # Replace NaN values with empty strings
    df = df.fillna('None')
    
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
    
    return df

def parse_date(date_str):
    """Convert date string to datetime object."""
    date_formats = [
        '%d-%b-%y',    # Original format (01-Jun-25)
        '%b %d, %Y',   # New format (May 11, 2025)
        '%Y-%m-%d'     # Fallback format (2025-05-11)
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

def identify_weeks_in_month(dates):
    """
    Identify all weeks (Saturday to Friday) within the given dates.
    Returns list of (start_date, end_date) tuples for each week.
    """
    if not dates:
        return []
    
    # Filter out None values and find min/max dates
    valid_dates = [d for d in dates if d is not None]
    if not valid_dates:
        return []
    
    min_date = min(valid_dates)
    max_date = max(valid_dates)
    
    # Get the first day of the month
    first_day = datetime(min_date.year, min_date.month, 1)
    
    # Find the first Saturday (start of first week)
    days_until_saturday = (5 - first_day.weekday()) % 7
    first_saturday = first_day + timedelta(days=days_until_saturday)
    
    # If first_saturday is after max_date, no complete weeks
    if first_saturday > max_date:
        return [(first_day, max_date)]
    
    # Handle dates before the first Saturday
    weeks = []
    if first_saturday > first_day:
        weeks.append((first_day, first_saturday - timedelta(days=1)))
    
    # Generate all Saturday-Friday weeks
    current_saturday = first_saturday
    while current_saturday <= max_date:
        friday = current_saturday + timedelta(days=6)
        if friday > max_date:
            friday = max_date
        
        weeks.append((current_saturday, friday))
        current_saturday = friday + timedelta(days=1)
    
    return weeks

def group_data_by_date_and_name(df):
    """
    Group data by date, and within each date group by deceased name.
    Blanks out repeated fields according to the rules.
    """
    # Create a copy of the dataframe
    result_df = df.copy()
    
    # Add grouping columns
    result_df['date_group'] = (result_df['Date'] != result_df['Date'].shift()).cumsum()
    
    # Process each date group
    for group_id in result_df['date_group'].unique():
        group_mask = result_df['date_group'] == group_id
        group_indices = result_df.index[group_mask].tolist()
        
        # Blank out date fields for all but the first row in each date group
        if len(group_indices) > 1:
            for idx in group_indices[1:]:
                result_df.at[idx, 'Day of the Week'] = ''
                result_df.at[idx, 'Date'] = ''
                result_df.at[idx, 'Hebrew Day'] = ''
                result_df.at[idx, 'Hebrew Month'] = ''
        
        # Create a sub-grouping for deceased names within this date group
        sub_df = result_df[group_mask].copy()
        sub_df['deceased_name'] = sub_df['Deceased Last Name'] + '|' + sub_df['Deceased First Name']
        sub_df['name_group'] = (sub_df['deceased_name'] != sub_df['deceased_name'].shift()).cumsum()
        
        # Process each name group within the date group
        for name_group_id in sub_df['name_group'].unique():
            name_mask = sub_df['name_group'] == name_group_id
            name_indices = sub_df.index[name_mask].tolist()
            
            # Blank out name fields for all but the first row in each name group
            if len(name_indices) > 1:
                for idx in name_indices[1:]:
                    result_df.at[idx, 'Deceased First Name'] = ''
                    result_df.at[idx, 'Deceased Last Name'] = ''
    
    # Remove helper columns
    result_df = result_df.drop(columns=['date_group'])
    
    return result_df

def create_weekly_dataframes(df):
    """
    Create separate dataframes for each week and a master dataframe.
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
    
    # Identify weeks in the month
    weeks = identify_weeks_in_month([d for d in date_objects if d is not None])
    
    if not weeks:
        print("No valid weeks found in the data")
        df_sorted = df_sorted.drop(columns=['_date_obj'])
        return [df_sorted], ["Complete List"], df_sorted, "Yahrzeit List"
    
    # Get month and year for title from the first date
    first_date = min(d for d in date_objects if d is not None)
    spreadsheet_title = f"{first_date.strftime('%B-%Y')} Yahrzeit List"
    
    # Create weekly dataframes
    weekly_dfs = []
    sheet_names = []
    
    for start_date, end_date in weeks:
        # Filter data for this week
        weekly_data = df_sorted[
            (df_sorted['_date_obj'] >= start_date) & 
            (df_sorted['_date_obj'] <= end_date)
        ].copy()
        
        if not weekly_data.empty:
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

def connect_to_google_sheets():
    """Authenticate and connect to Google Sheets API using OAuth2."""
    try:
        # Define the scopes
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        
        # Files for authentication
        TOKEN_FILE = 'token.json'
        CLIENT_SECRETS_FILE = 'client_secret.json'
        
        creds = None
        
        # Check if we have valid stored credentials
        if os.path.exists(TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        
        # If no valid credentials, request authentication
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(CLIENT_SECRETS_FILE):
                    print(f"ERROR: {CLIENT_SECRETS_FILE} not found.")
                    print("Please download your OAuth credentials from Google Cloud Console")
                    print("and save them as 'client_secret.json' in this directory.")
                    return None, None
                
                # Try local server authentication
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
                    creds = flow.run_local_server(port=8080)
                except Exception as local_error:
                    print(f"Local server authentication failed: {local_error}")
                    print("Trying manual authentication instead...")
                    
                    # Fall back to manual authentication
                    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
                    flow.redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'
                    
                    auth_url, _ = flow.authorization_url(prompt='consent')
                    print("\n" + "="*80)
                    print("Please open this URL in your browser to authenticate:")
                    print(auth_url)
                    print("="*80 + "\n")
                    
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
        print(f"Error connecting to Google Sheets API: {str(e)}")
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
        print(f"Error creating spreadsheet: {e}")
        return None

def format_sheet(service, spreadsheet_id, sheet_id):
    """Apply formatting to a sheet with Arial 12 bold font and grid lines."""
    try:
        requests = []
        
        # Format header row - bold, center, and gray background
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'CENTER',
                        'textFormat': {
                            'fontFamily': 'Arial',
                            'fontSize': 12,
                            'bold': True
                        },
                        'backgroundColor': {
                            'red': 0.9,
                            'green': 0.9,
                            'blue': 0.9
                        },
                        'wrapStrategy': 'OVERFLOW_CELL'
                    }
                },
                'fields': 'userEnteredFormat(horizontalAlignment,textFormat,backgroundColor,wrapStrategy)'
            }
        })
        
        # Apply Arial font size 12 bold to all data cells and center align
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1  # Start after header
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'CENTER',
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
        print(f"Error formatting sheet: {e}")

def add_borders_and_resize_columns(service, spreadsheet_id, sheet_id, values):
    """Add borders to all cells with content and resize columns to fit content."""
    try:
        if not values or len(values) == 0:
            return

        # Find the maximum row and column with content
        max_row = len(values)
        max_col = max(len(row) for row in values)
        
        # Calculate column widths based on content
        column_widths = {}
        
        for row in values:
            for col_idx, cell_value in enumerate(row):
                if cell_value:
                    # Calculate width based on content length (8 pixels per character + padding)
                    content_length = len(str(cell_value))
                    width = max(column_widths.get(col_idx, 0), content_length)
                    column_widths[col_idx] = width
        
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
        for col_idx, width in column_widths.items():
            # Convert to Google Sheets column width (pixels ÷ 8)
            pixel_width = width * 8 + 30  # 8 pixels per character + 30 pixels padding
            
            # Ensure minimum width
            pixel_width = max(pixel_width, 100)
            
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
        print(f"Error adding borders and resizing columns: {e}")

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
        # Create a new spreadsheet
        spreadsheet_id = create_spreadsheet(drive_service, spreadsheet_title)
        if not spreadsheet_id:
            print("Failed to create spreadsheet")
            return
        
        # Get the default sheet ID
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        default_sheet_id = spreadsheet['sheets'][0]['properties']['sheetId']
        
        # Rename the default sheet to "Master"
        requests = [{
            'updateSheetProperties': {
                'properties': {
                    'sheetId': default_sheet_id,
                    'title': 'Master'
                },
                'fields': 'title'
            }
        }]
        
        # Create additional sheets for weekly data
        for name in sheet_names:
            requests.append({
                'addSheet': {
                    'properties': {
                        'title': name
                    }
                }
            })
        
        # Execute batch update to create sheets
        body = {'requests': requests}
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        
        # Get sheet IDs for the newly created sheets
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_properties = {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in spreadsheet['sheets']}
        
        # Populate Master sheet
        master_values = prepare_data_for_sheets(master_df)
        body = {'values': master_values}
        
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Master!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # Format Master sheet
        format_sheet(service, spreadsheet_id, sheet_properties['Master'])
        add_borders_and_resize_columns(service, spreadsheet_id, sheet_properties['Master'], master_values)
        
        # Populate and format weekly sheets
        for weekly_df, sheet_name in zip(weekly_sheets, sheet_names):
            # Prepare and upload data
            weekly_values = prepare_data_for_sheets(weekly_df)
            body = {'values': weekly_values}
            
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A1",
                valueInputOption='RAW',
                body=body
            ).execute()
            
            # Format sheet
            format_sheet(service, spreadsheet_id, sheet_properties[sheet_name])
            add_borders_and_resize_columns(service, spreadsheet_id, sheet_properties[sheet_name], weekly_values)
        
        print(f"Successfully created spreadsheet: {spreadsheet_title}")
        print(f"Access your spreadsheet at: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        
    except Exception as e:
        print(f"Error creating and populating sheets: {e}")
        import traceback
        traceback.print_exc()

def main():
    # File path to your CSV
    csv_file = "test/test2.csv"
    
    # Check if files exist
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found.")
        return
    
    if not os.path.exists('client_secret.json'):
        print("Error: Client secrets file 'client_secret.json' not found.")
        print("Please ensure your OAuth2 client ID JSON file is named 'client_secret.json'")
        return
    
    print("Starting Yahrzeit List processing...")
    
    # Load and process data
    print("Loading CSV data...")
    df = load_data_from_csv(csv_file)
    
    print("Cleaning data...")
    df = clean_data(df)
    
    print("Identifying weeks and creating sheet data...")
    weekly_sheets, sheet_names, master_df, spreadsheet_title = create_weekly_dataframes(df)
    
    # Connect to Google Sheets API
    print("Authenticating with Google Sheets API...")
    print("A browser window may open for authentication if needed.")
    service, drive_service = connect_to_google_sheets()
    if not service or not drive_service:
        print("Failed to connect to Google Sheets API")
        return
    
    # Create and format spreadsheet
    print(f"Creating Google Sheet: {spreadsheet_title}...")
    create_and_populate_sheets(service, drive_service, master_df, weekly_sheets, sheet_names, spreadsheet_title)
    
    print("Script completed successfully.")

if __name__ == "__main__":
    main()