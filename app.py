from flask import Flask, request
import ibm_db
import json
import uuid

app = Flask(__name__)
try:
    print("Connecting")
    conn=ibm_db.connect('DATABASE=bludb;HOSTNAME=6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud;PORT=30376;SECURITY=SSL;SSLServerCertificate=DigiCertGlobalRootCA.crt;UID=ncv90043;PWD=l6SXXm78Bc5lPuP0','','')
    print("Successfully connected")
except Exception as e:
    print(ibm_db.conn_errormsg())

@app.route('/login', methods = ['POST'])
def login():
    email = request.form['email']
    password = request.form['password']
    try:
        stmt = ibm_db.exec_immediate(conn, f'select * from users where email = {email} and password = {password}')
        result = ibm_db.fetch_assoc(stmt)
        if result:
            response = app.response_class(
            response=json.dumps({"user_id":result['USER_ID']}),
            status=200,
            mimetype='application/json'
            )
            return response
        else:
            response = app.response_class(
            response=json.dumps('User Not Found'),
            status=404,
            mimetype='application/json'
            )
        return response
    except Exception as e:
        print(e)
        response = app.response_class(
            response=json.dumps({"message":str(e)}),
            status=400,
            mimetype='application/json'
        )
        return response



@app.route('/register', methods= ['POST'])
def register():
    name = request.form['name']
    email = request.form['email']
    password = request.form['password']
    limit = request.form['monthly_limit']
    try:
        id = "".join([n for n in str(uuid.uuid4())[:8] if n.isdigit()])
        stmt = ibm_db.exec_immediate(conn, f'INSERT into users values({int(id)},{name},{email},{password},{limit})')
        print("Number of affected rows: ", ibm_db.num_rows(stmt))
        stmt = ibm_db.exec_immediate(conn, f'SELECT * from users where email = {email} and password = {password}')
        result = ibm_db.fetch_assoc(stmt)
        response = app.response_class(
            response=json.dumps({"user_id":result["USER_ID"]}),
                status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        print(e)
        response = app.response_class(
            response=json.dumps({"user_id":None}),
            status=400,
            mimetype='application/json'
        )
        return response
    
@app.route('/add', methods = ['POST'])
def add_expense():
    date = request.form['date']
    amount = request.form['amount']
    category_id = request.form['category_id']
    description = request.form['description']
    expense_type = request.form['expense_type']

    user_id = request.headers['user_id']
    print("user_id got is ",user_id)
    try:
        id = "".join([n for n in str(uuid.uuid4())[:8] if n.isdigit()])
        stmt = ibm_db.exec_immediate(conn, f'SELECT expense_id from FINAL TABLE (INSERT INTO expense values ({id},{date},{amount},{category_id},{description},{expense_type}))')
        expense_id = ibm_db.fetch_assoc(stmt)['EXPENSE_ID']
        ibm_db.exec_immediate(conn, f'INSERT INTO user_expense VALUES ({user_id},{expense_id})')
        response = app.response_class(
            response=json.dumps({'message':'expense added successfully'}),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        print(e)
        response = app.response_class(
            response=json.dumps('400'),
            status=400,
            mimetype='application/json'
        )
        return response

@app.route('/categories', methods = ['GET'])
def get_categories():
    try:
        stmt = ibm_db.exec_immediate(conn, 'SELECT * from category')
        result = ibm_db.fetch_assoc(stmt)
        categories=[]
        while result != False:
            categories.append({'category_id':result['CATEGORY_ID'],'category_name':result['CATEGORY_NAME']})
            result = ibm_db.fetch_assoc(stmt)
        response = app.response_class(
            response=json.dumps(categories),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        response = app.response_class(
            response=json.dumps(str(e)),
            status=400,
            mimetype='application/json'
        )
        return response

@app.route('/expenses', methods = ['GET'])
def get_expenses():
    user_id = request.headers['user_id']
    type = None
    if request.args:
        type = request.args['type']
    try:
        sql = f'SELECT e.expense_id, e.amount, e.date, c.category_name, e.expense_type, e.description FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id  where u.user_id = {user_id}'
        if type:
            sql +=f'AND e.expense_type = {type}'

        stmt = ibm_db.exec_immediate(conn, sql )
        expense = ibm_db.fetch_assoc(stmt)
        expenses = []
        while expense !=False:
            exp =  {k.lower(): v for k, v in expense.items()}
            expenses.append(exp)
            expense = ibm_db.fetch_assoc(stmt)
        response = app.response_class(
            response=json.dumps(expenses),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        response = app.response_class(
            response=json.dumps(str(e)),
            status=400,
            mimetype='application/json'
        )
        return response

if __name__ == '__main__':
    app.run(debug = True)