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

        required_central_cols = ['Valid From', 'Supplier Name']
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
        central_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)
        pmd_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)
        app.logger.info("Date columns normalized and rows with missing comparison data dropped.")

        central_df['comp_key'] = central_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' + central_df['Supplier Name'].astype(str)
        pmd_df['comp_key'] = pmd_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' + pmd_df['Supplier Name'].astype(str)
        app.logger.info("Comparison keys created for both DataFrames (Valid From AND Supplier Name).")

        unique_pmd_rows = pmd_df[~pmd_df['comp_key'].isin(central_df['comp_key'])].copy()
        app.logger.info(f"Identified {len(unique_pmd_rows)} unique rows from PMD Lookup Data for output.")

        # --- Output File Generation ---
        output_required_cols = [
            'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street',
            'City', 'Country', 'Zip Code', 'Requested By', 'Pur. approver',
            'Pur. release date'
        ]

        # Filter the unique_pmd_rows to only include the required output columns
        available_output_cols = [col for col in output_required_cols if col in unique_pmd_rows.columns]
        final_output_df = unique_pmd_rows[available_output_cols].copy() 
        
        # Format 'Valid From' if 'Valid From_dt' exists in unique_pmd_rows
        # This needs to be done *before* dropping 'Valid From_dt' if the original 'Valid From' column is desired
        if 'Valid From_dt' in unique_pmd_rows.columns:
            final_output_df['Valid From'] = unique_pmd_rows['Valid From_dt'].dt.strftime('%Y-%m-%d %I:%M %p')
        
        # Clean up helper columns (comp_key and Valid From_dt)
        # Ensure 'Valid From_dt' is dropped if it was an internal helper column, not the original 'Valid From'
        final_output_df.drop(columns=['comp_key', 'Valid From_dt'], errors='ignore', inplace=True)
        
        # Reorder columns to match output_required_cols
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