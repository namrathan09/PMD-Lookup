from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
import os
import io
import logging

app = Flask(__name__)
app.secret_key = 'your_secret_key_here' # IMPORTANT: Change this to a strong, random key in production!

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Allowed file extensions for uploads
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}

def allowed_file(filename):
    """
    Checks if the uploaded filename has an allowed Excel extension.
    """
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET'])
def index():
    """
    Renders the main page with the file upload forms.
    """
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    """
    Handles the file uploads, processes the data, and returns an Excel file.
    """
    # 1. Validate file uploads
    if 'central_file' not in request.files or 'pmd_lookup_file' not in request.files:
        flash('One or both files are missing from the upload.', 'error')
        return redirect(url_for('index'))

    central_file = request.files['central_file']
    pmd_lookup_file = request.files['pmd_lookup_file']

    if central_file.filename == '' or pmd_lookup_file.filename == '':
        flash('Please select both a Central File and a PMD Lookup Data File.', 'error')
        return redirect(url_for('index'))

    if not (allowed_file(central_file.filename) and allowed_file(pmd_lookup_file.filename)):
        flash('Invalid file type. Please upload .xls or .xlsx files only for both files.', 'error')
        return redirect(url_for('index'))

    try:
        app.logger.info("Starting file processing...")

        # 2. Read Excel files into Pandas DataFrames
        central_df = pd.read_excel(io.BytesIO(central_file.read()))
        pmd_df = pd.read_excel(io.BytesIO(pmd_lookup_file.read()))
        app.logger.info("Files successfully read into DataFrames.")

        # --- PMD Lookup Data File Initial Processing ---
        # Drop specified columns from PMD Lookup DataFrame if they exist
        cols_to_drop = ['Sl. No.', 'DUNS']
        pmd_df = pmd_df.drop(columns=[col for col in cols_to_drop if col in pmd_df.columns], errors='ignore')
        app.logger.info(f"Dropped columns {cols_to_drop} from PMD Lookup DataFrame.")

        # Remove rows where 'Country' is in the specified exclusion list
        countries_to_exclude = ['CN', 'ID', 'TW', 'HK', 'JP', 'KR', 'MY', 'PH', 'SG', 'TH', 'VN']
        if 'Country' in pmd_df.columns:
            initial_rows_pmd = len(pmd_df)
            pmd_df = pmd_df[~pmd_df['Country'].isin(countries_to_exclude)]
            app.logger.info(f"Removed {initial_rows_pmd - len(pmd_df)} rows from PMD where 'Country' was in {countries_to_exclude}.")

        # --- Required Columns Check ---
        # 'Status' and 'Assigned To' are required in the Central File for comparison and data fetching
        required_central_cols_for_check = ['Valid From', 'Supplier Name', 'Status', 'Assigned To']
        required_pmd_cols_for_check = ['Valid From', 'Supplier Name']

        if not all(col in central_df.columns for col in required_central_cols_for_check):
            flash(f"Central File is missing one or more required columns: {required_central_cols_for_check}. Please ensure 'Assigned To' is present.", 'error')
            return redirect(url_for('index'))
        if not all(col in pmd_df.columns for col in required_pmd_cols_for_check):
            flash(f"PMD Lookup Data File is missing one or more required columns: {required_pmd_cols_for_check}.", 'error')
            return redirect(url_for('index'))

        # 3. Date Normalization and Data Cleaning
        central_df['Valid From_dt'] = pd.to_datetime(central_df['Valid From'], errors='coerce')
        pmd_df['Valid From_dt'] = pd.to_datetime(pmd_df['Valid From'], errors='coerce')

        # Drop rows where essential comparison/output data is missing
        central_df.dropna(subset=['Valid From_dt', 'Supplier Name', 'Status', 'Assigned To'], inplace=True)
        pmd_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)
        app.logger.info("Date columns normalized and rows with missing comparison data or required 'Status'/'Assigned To' dropped.")

        # 4. Create Comparison Keys
        central_df['comp_key'] = central_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' + central_df['Supplier Name'].astype(str)
        pmd_df['comp_key'] = pmd_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' + pmd_df['Supplier Name'].astype(str)
        app.logger.info("Comparison keys created for both DataFrames (Valid From AND Supplier Name).")

        # --- 5. Status Logic & Assigned To Fetching ---
        # Perform a left merge from pmd_df to central_df to bring in Central's Status and Assigned To
        # for matching records based on the composite key.
        merged_df = pd.merge(pmd_df,
                             central_df[['comp_key', 'Status', 'Assigned To']].rename(
                                 columns={'Status': 'Status_central', 'Assigned To': 'Assigned To_central'}),
                             on='comp_key',
                             how='left')
        app.logger.info("PMD Lookup Data merged with Central File's status and 'Assigned To' information based on comp_key.")

        # Define the function to determine the final status and assigned person
        def determine_final_output_details(row):
            # Scenario 1: No match found in central_df (Status_central is NaN)
            if pd.isna(row['Status_central']):
                return 'New', None # Status 'New', no 'Assigned To'

            # Scenarios 2 & 3: Match found, check central status
            else:
                # Scenario 2: Match found AND Central Status is "Approved" (case-insensitive)
                if isinstance(row['Status_central'], str) and row['Status_central'].lower() == 'approved':
                    return None, None # This row should be ignored, so return None for both status and assigned_to
                # Scenario 3: Match found BUT Central Status is NOT "Approved"
                else:
                    # Status is 'Hold', and fetch 'Assigned To' from the central file's corresponding record
                    return 'Hold', row['Assigned To_central']

        # Apply the function to create 'final_status' and 'final_assigned_to' columns
        merged_df[['final_status', 'final_assigned_to']] = merged_df.apply(
            lambda row: determine_final_output_details(row), axis=1, result_type='expand'
        )
        app.logger.info("Calculated 'New', 'Hold', or 'None' for each PMD record, and fetched 'Assigned To' where applicable.")

        # Filter out rows that should be ignored (where final_status is None)
        final_output_df = merged_df[merged_df['final_status'].notna()].copy()

        # Assign the calculated 'final_status' and 'final_assigned_to' to the output DataFrame's new columns
        final_output_df['Status'] = final_output_df['final_status']
        final_output_df['Assigned To'] = final_output_df['final_assigned_to']
        app.logger.info(f"Filtered to {len(final_output_df)} records for final output after applying status logic.")

        # --- 6. Output File Generation ---
        # Define the desired order and inclusion of columns for the final output Excel
        output_required_cols = [
            'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street',
            'City', 'Country', 'Zip Code', 'Requested By', 'Pur. approver',
            'Pur. release date', 'Status', 'Assigned To' # 'Assigned To' is now included
        ]

        # Ensure 'Valid From' column is formatted correctly for the output.
        # It takes the datetime object we created earlier and formats it as a string.
        if 'Valid From_dt' in final_output_df.columns:
            final_output_df['Valid From'] = final_output_df['Valid From_dt'].dt.strftime('%Y-%m-%d %I:%M %p')

        # Clean up helper columns created during processing
        columns_to_drop_after_status_calc = [
            'comp_key', 'Valid From_dt', 'Valid From_dt_pmd', 'Valid From_dt_central', # Valid From_dt_pmd/central might exist due to suffixes
            'Status_central', 'Assigned To_central', 'final_status', 'final_assigned_to'
        ]

        final_output_df.drop(columns=[col for col in columns_to_drop_after_status_calc if col in final_output_df.columns],
                             errors='ignore',
                             inplace=True)

        # Select and reorder columns to match 'output_required_cols'
        final_output_df = final_output_df[[col for col in output_required_cols if col in final_output_df.columns]]

        app.logger.info("Final output DataFrame prepared and formatted.")

        # 7. Create an in-memory Excel file and send it for download
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_output_df.to_excel(writer, index=False, sheet_name='Comparison_Result')
        output.seek(0) # Reset stream position to the beginning
        app.logger.info("Result Excel file created in memory.")

        flash('Files processed successfully! Your download should start shortly.', 'success')
        return send_file(output, as_attachment=True, download_name='PMD_Lookup_ResultFile.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    # --- Error Handling ---
    except KeyError as e:
        app.logger.error(f"Missing expected column in one of the Excel files: {e}", exc_info=True)
        flash(f"Error: One of the uploaded files is missing a required column: '{e}'. Please check your file headers. Required columns for Central File include 'Valid From', 'Supplier Name', 'Status', 'Assigned To'. For PMD Lookup: 'Valid From', 'Supplier Name'.", 'error')
        return redirect(url_for('index'))
    except pd.errors.EmptyDataError:
        app.logger.error("Uploaded Excel file is empty or unreadable.", exc_info=True)
        flash("Error: One of the uploaded Excel files appears to be empty or unreadable.", 'error')
        return redirect(url_for('index'))
    except Exception as e:
        app.logger.error(f'An unexpected error occurred during processing: {e}', exc_info=True)
        flash(f'An unexpected error occurred: {e}', 'error')
        return redirect(url_for('index'))

if __name__ == '__main__':
    # Run the Flask app in debug mode.
    # IMPORTANT: Set debug=False in a production environment!
    app.run(debug=True)
