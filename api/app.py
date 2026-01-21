from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
import os
import io
import logging

app = Flask(__name__)
app.secret_key = 'your_secret_key_here' 

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

ALLOWED_EXTENSIONS = {'xls', 'xlsx'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
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
        central_df = pd.read_excel(io.BytesIO(central_file.read()))
        pmd_df = pd.read_excel(io.BytesIO(pmd_lookup_file.read()))
        app.logger.info("Files successfully read into DataFrames.")

        # --- PMD Lookup Data File Processing ---
        cols_to_drop = ['Sl. No.', 'DUNS']
        pmd_df = pmd_df.drop(columns=[col for col in cols_to_drop if col in pmd_df.columns], errors='ignore')
        app.logger.info(f"Dropped columns {cols_to_drop} from PMD Lookup DataFrame.")

        # Remove rows where 'Country' is in the specified list (CN, ID, TW, HK, JP, KR, MY, PH, SG, TH, VN)
        countries_to_exclude = ['CN', 'ID', 'TW', 'HK', 'JP', 'KR', 'MY', 'PH', 'SG', 'TH', 'VN']
        if 'Country' in pmd_df.columns:
            initial_rows = len(pmd_df)
            pmd_df = pmd_df[~pmd_df['Country'].isin(countries_to_exclude)]
            app.logger.info(f"Removed {initial_rows - len(pmd_df)} rows where 'Country' was in {countries_to_exclude}.")

        # --- Updated Required Columns ---
        # 'Status' column is now required in the Central File for comparison logic
        required_central_cols = ['Valid From', 'Supplier Name', 'Status'] 
        required_pmd_cols = ['Valid From', 'Supplier Name']

        if not all(col in central_df.columns for col in required_central_cols):
            flash(f"Central File is missing one or more required columns: {required_central_cols}.", 'error')
            return redirect(url_for('index'))
        if not all(col in pmd_df.columns for col in required_pmd_cols):
            flash(f"PMD Lookup Data File is missing one or more required columns: {required_pmd_cols}.", 'error')
            return redirect(url_for('index'))

        central_df['Valid From_dt'] = pd.to_datetime(central_df['Valid From'], errors='coerce')
        pmd_df['Valid From_dt'] = pd.to_datetime(pmd_df['Valid From'], errors='coerce')
            
        # Drop rows where essential comparison data is missing (NaT for date, or NaN for Supplier Name)
        # Added 'Status' for NA check in central_df as it's now critical for logic
        central_df.dropna(subset=['Valid From_dt', 'Supplier Name', 'Status'], inplace=True)
        pmd_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)
        app.logger.info("Date columns normalized and rows with missing comparison data and required 'Status' dropped.")

        central_df['comp_key'] = central_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' + central_df['Supplier Name'].astype(str)
        pmd_df['comp_key'] = pmd_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' + pmd_df['Supplier Name'].astype(str)
        app.logger.info("Comparison keys created for both DataFrames (Valid From AND Supplier Name).")

        # --- New Status Logic ---
        # Perform a left merge from pmd_df to central_df to bring in Central's Status for matching records
        # This allows us to check Central's status even for rows that "match"
        merged_df = pd.merge(pmd_df, central_df[['comp_key', 'Status']], 
                             on='comp_key', 
                             how='left', 
                             suffixes=('_pmd', '_central'))
        app.logger.info("PMD Lookup Data merged with Central File's status information based on comp_key.")

        # Determine the 'final_status' for each row based on the new rules
        def determine_final_status(row):
            # Scenario 1: No match found in central_df (Status_central is NaN)
            if pd.isna(row['Status_central']):
                return 'New'
            # Scenarios 2 & 3: Match found, check central status
            else:
                # Scenario 2: Match found AND Central Status is "Approved" (case-insensitive)
                if row['Status_central'].lower() == 'approved': 
                    return None # This row should be ignored, so we return None
                # Scenario 3: Match found BUT Central Status is NOT "Approved"
                else:
                    return 'Hold' # Include this row, status is "Hold"

        merged_df['final_status'] = merged_df.apply(determine_final_status, axis=1)
        app.logger.info("Calculated 'New', 'Hold', or 'None' for each PMD record based on matching and Central Status.")

        # Filter out rows that should be ignored (where final_status is None)
        final_output_df = merged_df[merged_df['final_status'].notna()].copy()

        # Assign the calculated 'final_status' to the new 'Status' column in the output DataFrame
        final_output_df['Status'] = final_output_df['final_status'] 
        app.logger.info(f"Filtered to {len(final_output_df)} records for final output after applying status logic.")

        # --- Output File Generation ---
        output_required_cols = [
            'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street',
            'City', 'Country', 'Zip Code', 'Requested By', 'Pur. approver',
            'Pur. release date', 'Status' # Added 'Status' to output columns
        ]

        # Select columns that originated from pmd_df (excluding comp_key, Valid From_dt_pmd, Status_central, final_status)
        # and then add the new 'Status' column
        
        # We need to ensure we're working with the columns from the original PMD file
        # plus the newly created 'Status' column.
        # Let's get the original PMD columns for selection.
        original_pmd_columns = [col for col in pmd_df.columns if col not in ['comp_key', 'Valid From_dt']]
        
        # Ensure 'Valid From' is formatted correctly before final selection
        # Check if the Valid From from the pmd_df is a datetime object after initial processing
        if 'Valid From_dt_pmd' in final_output_df.columns and final_output_df['Valid From_dt_pmd'].dtype == 'datetime64[ns]':
            final_output_df['Valid From'] = final_output_df['Valid From_dt_pmd'].dt.strftime('%Y-%m-%d %I:%M %p')
        elif 'Valid From_dt' in final_output_df.columns and final_output_df['Valid From_dt'].dtype == 'datetime64[ns]': # Fallback for if suffix wasn't added due to no conflict
             final_output_df['Valid From'] = final_output_df['Valid From_dt'].dt.strftime('%Y-%m-%d %I:%M %p')
        
        # Now drop all the temporary helper columns and the merged central status
        columns_to_drop_after_status_calc = [
            'comp_key', 'Valid From_dt_pmd', 'Valid From_dt_central', # from merged_df
            'Valid From_dt', # from pmd_df if suffix not applied
            'Status_central', 'final_status'
        ]
        final_output_df.drop(columns=[col for col in columns_to_drop_after_status_calc if col in final_output_df.columns], 
                             errors='ignore', 
                             inplace=True)
        
        # Reorder columns to match output_required_cols, including the new 'Status'
        final_output_df = final_output_df[[col for col in output_required_cols if col in final_output_df.columns]]
        
        app.logger.info("Final output DataFrame prepared and formatted.")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_output_df.to_excel(writer, index=False, sheet_name='Comparison_Result')
        output.seek(0)
        app.logger.info("Result Excel file created in memory.")

        flash('Files processed successfully! Your download should start shortly.', 'success')
        return send_file(output, as_attachment=True, download_name='PMD_Lookup_ResultFile.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except KeyError as e:
        app.logger.error(f"Missing expected column in one of the Excel files: {e}", exc_info=True)
        flash(f"Error: One of the uploaded files is missing a required column: '{e}'. Please check your file headers.", 'error')
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
    app.run(debug=True)
