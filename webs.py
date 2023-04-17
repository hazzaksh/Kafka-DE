from flask import Flask, render_template, request
from oops import App

app = Flask(__name__)
class_app = App()
# Define a list of options for the dropdown
options = ['balancesheet','profitloss', 'salesbyproduct', 'cashflow']

# Define a function to be invoked when the selection is made
def backend_function(selected_option):
    # Your backend logic goes here
    class_app.fun(selected_option)
    print(f'The user selected {selected_option}')

# Define a route to render the HTML template with the dropdown
@app.route('/', methods=['GET', 'POST'])
def index():
    
    if request.method == 'POST':
        selected_option = request.form['options']
        backend_function(selected_option)
    return render_template('index.html', options = options)

if __name__ == '__main__':
    app.run(debug=True)
