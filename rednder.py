from flask import Flask, render_template
import boto3
import csv

app = Flask(__name__)

# Set up the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAZIVCGE6ARU6GXGMR',
    aws_secret_access_key='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'

)
s3_folder_paths = {
    'balancesheet': 'c1/balancesheet',
    'profitloss': 'c1/profitloss',
    'salesbyproduct': 'c1/salesbyproduct',
    'cashflow': 'c1/cashflow'
}
@app.route('/')
def index():
    # Retrieve the CSV file from S3
    object_key = f"{s3_folder_paths['balancesheet']}/{9}.csv"
    response = s3.get_object(Bucket='kafka-server-bucket1', Key=object_key)
    csv_bytes = response['Body'].read()
    csv_data = csv_bytes.decode('utf-8')

    # Parse the CSV data into a list of rows
    rows = list(csv.reader(csv_data.split('\n'), delimiter=','))

    # Render the HTML template and pass the rows to it
    return render_template('render.html', rows=rows)

if __name__ == '__main__':
    app.run()
