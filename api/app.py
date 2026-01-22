from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
import io
import logging

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # change in production

# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

ALLOWED_EXTENSIONS = {'xls', 'xlsx'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/process', methods=['POST'])
def process_files():
    try:
        # -------------------- FILE VALIDATION --------------------
        if 'central_file' not in request.files or 'pmd_lookup_file' not in request.files:
            flash('Both files are required.', 'error')
            return redirect(url_for('index'))

        central_file = request.files['central_file']
        pmd_file = request.files['pmd_lookup_file']

        if central_file.filename == '' or pmd_file.filename == '':
            flash('Please select both files.', 'error')
            return redirect(url_for('index'))

        if not (allowed_file(central_file.filename) and allowed_file(pmd_file.filename)):
            flash('Only Excel files (.xls, .xlsx) are allowed.', 'error')
            return redirect(url_for('index'))

        # -------------------- READ FILES --------------------
        central_df = pd.read_excel(io.BytesIO(central_file.read()))
        pmd_df = pd.read_excel(io.BytesIO(pmd_file.read()))

        # -------------------- REQUIRED COLUMNS --------------------
        central_required = ['Valid From', 'Supplier Name', 'Status', 'Assigned']
        pmd_required = ['Valid From', 'Supplier Name']

        for col in central_required:
            if col not in central_df.columns:
                raise KeyError(f"Central file missing column: {col}")

        for col in pmd_required:
            if col not in pmd_df.columns:
                raise KeyError(f"PMD file missing column: {col}")

        # -------------------- DATE NORMALIZATION --------------------
        central_df['Valid From_dt'] = pd.to_datetime(central_df['Valid From'], errors='coerce')
        pmd_df['Valid From_dt'] = pd.to_datetime(pmd_df['Valid From'], errors='coerce')

        central_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)
        pmd_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)

        # -------------------- CREATE MATCH KEY --------------------
        central_df['comp_key'] = (
            central_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' +
            central_df['Supplier Name'].astype(str).str.strip()
        )

        pmd_df['comp_key'] = (
            pmd_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' +
            pmd_df['Supplier Name'].astype(str).str.strip()
        )

        # -------------------- CENTRAL LOOKUP (NO JOIN) --------------------
        central_lookup = central_df.set_index('comp_key')[['Status', 'Assigned']]

        # -------------------- BUSINESS LOGIC --------------------
        def determine_status(row):
            # No match → New
            if row['comp_key'] not in central_lookup.index:
                return 'New', None

            central_status = central_lookup.loc[row['comp_key'], 'Status']
            central_assigned = central_lookup.loc[row['comp_key'], 'Assigned']

            # Match + Approved → Ignore
            if isinstance(central_status, str) and central_status.lower() == 'approved':
                return None, None

            # Match + Not Approved → Hold
            return 'Hold', central_assigned

        pmd_df[['Status', 'Assigned']] = pmd_df.apply(
            lambda r: determine_status(r),
            axis=1,
            result_type='expand'
        )

        # Remove ignored rows
        final_df = pmd_df[pmd_df['Status'].notna()].copy()

        # -------------------- FORMAT & OUTPUT --------------------
        final_df['Valid From'] = final_df['Valid From_dt'].dt.strftime('%Y-%m-%d %I:%M %p')

        output_columns = [
            'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street',
            'City', 'Country', 'Zip Code', 'Requested By', 'Pur. approver',
            'Pur. release date', 'Status', 'Assigned'
        ]

        final_df = final_df[[col for col in output_columns if col in final_df.columns]]

        # -------------------- CREATE EXCEL --------------------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False, sheet_name='Result')

        output.seek(0)

        flash('File processed successfully!', 'success')
        return send_file(
            output,
            as_attachment=True,
            download_name='PMD_Lookup_Result.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        logging.error(str(e), exc_info=True)
        flash(f"Error: {e}", 'error')
        return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=True)
