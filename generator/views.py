from django.shortcuts import render
import boto3
import json
from datetime import date, timedelta
from pineline_lambdas.settings import ACCESS_KEY, SECRET_ACCESS_KEY
from pineline_lambdas.settings import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
import psycopg2

# Create your views here.
def home(request):
    return render(request, 'home.html')


def runPineline(request):
    print('ejecuto')
    start_date = date.today() - timedelta(days=1)
    if request.GET.get('start_date'):
        start_date = request.GET.get('start_date')

    end_date = date.today().strftime("%Y-%m-%d")
    if request.GET.get('end_date'):
        end_date = request.GET.get('end_date')
    
    print(start_date)
    print(end_date)
    topicArn = 'arn:aws:sns:us-east-1:208060198737:TransactionTopic'
    snsClient = boto3.client(
        'sns',
         aws_access_key_id=ACCESS_KEY,
         aws_secret_access_key=SECRET_ACCESS_KEY,
         region_name='us-east-1'
    )
    publishOject = {"function": "extraction", "start_date": str(start_date), "end_date": str(end_date)}
    response = snsClient.publish(TopicArn=topicArn,
                                 Message=json.dumps(publishOject),
                                 Subject='FUNCTION',
                                 MessageAttributes={"TransactionType": { "DataType": "String", "StringValue": "FUNCTION" }})

    status = response['ResponseMetadata']['HTTPStatusCode']
    message = ''
    if status == 200:
        message = 'Pipeline ejecutado correctamente'
    else:
        message = 'error al ejecutar el pipeline'
    

    conn = psycopg2.connect(
                    host= DB_HOST, 
                    dbname = DB_NAME, 
                    user = DB_USER,
                    password = DB_PASS,
                    port = DB_PORT)

    cur = conn.cursor()
    cur.execute("SELECT name,is_potentially_hazardous_asteroid FROM Object")
    rows = cur.fetchall()
    objects = []
    for row in rows:
        objects.append({
            'name': row[0],
            'is_potentially_hazardous_asteroid': row[1]
        })
    return render(request, 'home.html', {'message': message, 'objects': objects})