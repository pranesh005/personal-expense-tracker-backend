from flask import Flask, request
from flask_cors import CORS, cross_origin
import ibm_db
import json
import uuid
import datetime
from datetime import datetime, timedelta, date
import calendar

app = Flask(__name__)
cors = CORS(app)
try:
    print("Connecting")
    conn=ibm_db.connect('DATABASE=bludb;HOSTNAME=6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud;PORT=30376;SECURITY=SSL;SSLServerCertificate=DigiCertGlobalRootCA.crt;UID=ncv90043;PWD=l6SXXm78Bc5lPuP0','','')
    print("Successfully connected")
except Exception as e:
    print(ibm_db.conn_errormsg())
@app.route('/')
@cross_origin()
def hello():
    return 'hello'

@app.route('/login', methods = ['POST'])
@cross_origin()
def login():
    email = request.form['email']
    password = request.form['password']
    try:
        stmt = ibm_db.exec_immediate(conn, "select * from users where email = '%s' and password = '%s'" % (email,password))
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
@cross_origin()
def register():
    name = request.form['name']
    email = request.form['email']
    password = request.form['password']
    limit = request.form['monthly_limit']
    try:
        id = "".join([n for n in str(uuid.uuid4())[:8] if n.isdigit()])
        stmt = ibm_db.exec_immediate(conn, "select * from users where email = '%s'" % (email))
        print("num rows is ",ibm_db.num_rows(stmt))
        if ibm_db.fetch_assoc(stmt):
            response = app.response_class(
            response=json.dumps({"message":'Email already exists'}),
            status=409,
            mimetype='application/json'
            )
            print("already exists")
            return response
        print("new email")
        stmt = ibm_db.exec_immediate(conn, "INSERT into users values('%s','%s','%s','%s','%s')" % (int(id),name,email,password,limit))
        print("Number of affected rows: ", ibm_db.num_rows(stmt))
        stmt = ibm_db.exec_immediate(conn, "SELECT * from users where email = '%s' and password = '%s'" % (email,password))
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
@cross_origin()
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
        stmt = ibm_db.exec_immediate(conn, "SELECT expense_id from FINAL TABLE (INSERT INTO expense values ('%s','%s','%s','%s','%s','%s'))" % (int(id),date,amount,category_id,description,expense_type))
        expense_id = ibm_db.fetch_assoc(stmt)['EXPENSE_ID']
        ibm_db.exec_immediate(conn, "INSERT INTO user_expense VALUES ('%s','%s')" % (user_id,expense_id))
        response = app.response_class(
            response=json.dumps({'message':'expense added successfully'}),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        print(e)
        response = app.response_class(
            response=json.dumps(str(e)),
            status=400,
            mimetype='application/json'
        )
        return response

@app.route('/categories', methods = ['GET'])
@cross_origin()
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
@cross_origin()
def get_expenses():
    user_id = request.headers['user_id']
    type = None
    if request.args:
        type = request.args['type']
    try:
        sql = "SELECT e.expense_id, e.amount, e.date, c.category_name, e.expense_type, e.description FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id  where u.user_id = %s" % user_id
        if type:
            sql +=" AND e.expense_type = '%s'" % type
        sql+=" ORDER BY e.date DESC"

        stmt = ibm_db.exec_immediate(conn, sql )
        expense = ibm_db.fetch_assoc(stmt)
        expenses = []
        while expense !=False:
            exp =  {k.lower(): v for k, v in expense.items()}
            date = exp['date']
            exp['date'] = date.__str__()
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

@app.route('/expenditure-breakdown')
def expenditure_breakdown():
    user_id = request.headers['user_id']
    try:
        week_start_date, week_end_date = get_week_start_and_end()
        sql_week_spent = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id  where u.user_id = %s and e.expense_type = 'debit' and e.date between '%s' And '%s'" % (user_id,week_start_date,week_end_date)
        print(sql_week_spent)
        stmt = ibm_db.exec_immediate(conn, sql_week_spent)
        week_spent = ibm_db.fetch_assoc(stmt)
        if not week_spent:
            week_spent = 0
        else:
            week_spent = week_spent['1']

        today_start, today_end = get_today_datetime_start_and_end()
        sql_today_spent = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id  where u.user_id = %s and e.expense_type = 'debit' and e.date between '%s' And '%s'" % (user_id,today_start,today_end)
        stmt = ibm_db.exec_immediate(conn, sql_today_spent)
        today_spent = ibm_db.fetch_assoc(stmt)
        if not today_spent:
            today_spent = 0
        else:
            today_spent = today_spent['1']

        month_start, month_end = get_month_start_and_end()
        sql_month_spent = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id  where u.user_id = %s and e.expense_type = 'debit' and e.date between '%s' And '%s'" % (user_id,month_start,month_end)
        stmt = ibm_db.exec_immediate(conn, sql_month_spent)
        month_spent = ibm_db.fetch_assoc(stmt)
        if not month_spent:
            month_spent = 0
        else:
            month_spent = month_spent['1']

        year_start, year_end = get_year_start_and_end()
        sql_year_spent = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id  where u.user_id = %s and e.expense_type = 'debit' and e.date between '%s' And '%s'" % (user_id,year_start,year_end)
        stmt = ibm_db.exec_immediate(conn, sql_year_spent)
        year_spent = ibm_db.fetch_assoc(stmt)
        if not year_spent:
            year_spent = 0
        else:
            year_spent = year_spent['1']

        sql_total_spent = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id  where u.user_id = %s and e.expense_type = 'debit'" % (user_id)
        stmt = ibm_db.exec_immediate(conn, sql_total_spent)
        total_spent = ibm_db.fetch_assoc(stmt)
        if not total_spent:
            total_spent = 0
        else:
            total_spent = total_spent['1']

        most_spent_category= get_most_spent_on(user_id)
        if not most_spent_category:
            most_spent_category = 'Nil'

        print(most_spent_category)
        result={'week':week_spent,
                'today':today_spent,
                'month':month_spent,
                'year':year_spent,
                'total':total_spent,
                'most_spent_on':most_spent_category}
        response = app.response_class(
            response=json.dumps(result),
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

@app.route('/update-monthly-limit/<monthly_limit>', methods = ['PUT'])
def update_limit(monthly_limit):
    user_id = request.headers['user_id']
    try:
        sql_update = "UPDATE users SET MONTHLY_LIMIT = '%s' where user_id = %s" % (monthly_limit,user_id)
        stmt = ibm_db.exec_immediate(conn, sql_update)
        response = app.response_class(
            response=json.dumps({'message':'Updated Successfully'}),
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

@app.route('/delete-expense/<expense_id>', methods = ['DELETE'])
def delete_expense(expense_id):
    user_id = request.headers['user_id']
    try:
        sql_delete_expense = "DELETE FROM expense where expense_id = %s" % expense_id
        ibm_db.exec_immediate(conn, sql_delete_expense)

        sql_delete_user_expense = "DELETE FROM user_expense where expense_id = %s"% expense_id
        ibm_db.exec_immediate(conn, sql_delete_user_expense)
        response = app.response_class(
            response=json.dumps({'message':'Deleted Successfully'}),
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

@app.route('/update-expense/<expense_id>', methods = ['PUT'])
def update_expense(expense_id):
    user_id = request.headers['user_id']
    try:
        date = request.form['date']
        amount = request.form['amount']
        category_id = request.form['category_id']
        description = request.form['description']
        expense_type = request.form['expense_type']
        sql_update_expense = "UPDATE EXPENSE SET date = '%s', amount = %s, category_id = %s, description = '%s', expense_type = '%s' where expense_id = %s" % (date,amount,category_id,description,expense_type,expense_id)
        stmt = ibm_db.exec_immediate(conn, sql_update_expense)
        if ibm_db.num_rows(stmt) >0:
            response = app.response_class(
            response=json.dumps({'message':'Updated Successfully'}),
            status=200,
            mimetype='application/json'
            )
            return response
        else:
            response = app.response_class(
            response=json.dumps({'message':'Something went wrong. Expense not updated'}),
            status=400,
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

@app.route('/profile', methods = ['GET'])
def profile():
    user_id = request.headers['user_id']
    try:
        sql_get_profile = "SELECT * from users where user_id = %s"%user_id
        stmt = ibm_db.exec_immediate(conn, sql_get_profile)
        profile={}
        result = ibm_db.fetch_assoc(stmt)
        res =  {k.lower(): v for k, v in result.items()}
        res.pop('password')
        response = app.response_class(
            response=json.dumps(res),
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

@app.route('/chart', methods = ['GET'])
def chart():
    user_id =request.headers['user_id']
    try:
        month_start , month_end = get_month_start_and_end()
        categories_map = {1:'Food', 2:'Automobiles', 3:'Entertainment',4:'Clothing',5:'Healthcare',6:'Others'}
        sql_cat1 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id RIGHT JOIN category c ON e.category_id = c.category_id  where u.user_id = %s and e.category_id = 1 and e.date between '%s' And '%s'" % (user_id,month_start,month_end)
        sql_cat2 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 2 and e.date between '%s' And '%s'" % (user_id,month_start,month_end)
        sql_cat3 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 3 and e.date between '%s' And '%s'" % (user_id,month_start,month_end)
        sql_cat4 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 4 and e.date between '%s' And '%s'" % (user_id,month_start,month_end)
        sql_cat5 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 5 and e.date between '%s' And '%s'" % (user_id,month_start,month_end)
        sql_cat6 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 6 and e.date between '%s' And '%s'" % (user_id,month_start,month_end)
        queries = [sql_cat1,sql_cat2,sql_cat3,sql_cat4,sql_cat5,sql_cat6]

        chart_data = {}
        for index in range(0,6):
            stmt = ibm_db.exec_immediate(conn, queries[index])
            result = ibm_db.fetch_assoc(stmt)
            if result['1']:
                print(result)
                chart_data[categories_map[index+1]] = result['1']
            else:
                chart_data[categories_map[index+1]] = 0
        response = app.response_class(
                response=json.dumps(chart_data),
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
    


def get_week_start_and_end():
    day = str(date.today().strftime('%d/%b/%Y'))
    dt = datetime.strptime(day, '%d/%b/%Y')
    start = dt - timedelta(days=dt.weekday())
    end = start + timedelta(days=6)
    end = str(end).split(' ')[0]
    end+=' 24:00:00'
    return start, end

def get_today_datetime_start_and_end():
    today = str(date.today())
    today_start = today+' 00:00:00'
    today_end = today+' 24:00:00'
    return today_start,today_end

def get_month_start_and_end():
    currentDay = datetime.now().day
    currentMonth = datetime.now().month
    currentYear = datetime.now().year
    month_start= datetime(currentYear,currentMonth,1)
    end_date = calendar.monthrange(currentYear, currentMonth)[1]
    month_end = datetime(currentYear,currentMonth,end_date)
    return month_start,month_end

def get_year_start_and_end():
    currentYear = datetime.now().year
    year_start = datetime(currentYear,1,1)
    year_end = datetime(currentYear,12,31)
    return year_start, year_end

def get_most_spent_on(user_id):
    try:
        max_category = None
        max_id = 1
        max_amount = 0

        week_start, week_end = get_week_start_and_end()
        categories_map = {1:'Food', 2:'Automobiles', 3:'Entertainment',4:'Clothing',5:'Healthcare',6:'Others'}
        sql_cat1 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id RIGHT JOIN category c ON e.category_id = c.category_id  where u.user_id = %s and e.category_id = 1 and e.date between '%s' And '%s'" % (user_id,week_start,week_end)
        sql_cat2 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 2 and e.date between '%s' And '%s'" % (user_id,week_start,week_end)
        sql_cat3 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 3 and e.date between '%s' And '%s'" % (user_id,week_start,week_end)
        sql_cat4 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 4 and e.date between '%s' And '%s'" % (user_id,week_start,week_end)
        sql_cat5 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 5 and e.date between '%s' And '%s'" % (user_id,week_start,week_end)
        sql_cat6 = "SELECT SUM(e.amount) FROM expense e INNER JOIN user_expense u ON e.expense_id=u.expense_id FULL JOIN category c ON e.category_id = c.category_id where u.user_id = %s and e.expense_type = 'debit' and e.category_id = 6 and e.date between '%s' And '%s'" % (user_id,week_start,week_end)
        queries = [sql_cat1,sql_cat2,sql_cat3,sql_cat4,sql_cat5,sql_cat6]
        for index in range(0,6):
            print("index ",index)
            stmt = ibm_db.exec_immediate(conn, queries[index])
            result = ibm_db.fetch_assoc(stmt)
            if result['1']:
                print(result)
                if result['1'] > max_amount:
                    max_category = categories_map[index+1]
                    max_id=index
                    max_amount= result['1']
        print("max category is ",max_category)
        print("max amount is ",max_amount)
        return max_category
    except Exception as e:
        print(e)


if __name__ == '__main__':
    app.run(debug = True)