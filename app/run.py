from flask import Flask, request, send_file
import boto3

app = Flask(__name__)

# Connect to DynamoDB (ensure region is correct)
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

# Table references
student_table = dynamodb.Table('student')
marks_table = dynamodb.Table('marks')

@app.route('/')
def index():
    return send_file('index.html')

@app.route('/save_data', methods=['GET'])
def save_data():
    regno = request.args.get('regno')     # Will be used as 'id' in student table
    name = request.args.get('name')
    class_ = request.args.get('class')    # Will be used as sort key in marks table
    math = request.args.get('math')
    english = request.args.get('english')
    science = request.args.get('science')
    computer = request.args.get('computer')

    # Insert into student table
    student_table.put_item(
        Item={
            'id': regno,
            'name': name,
            'class': class_
        }
    )

    # Insert into marks table
    marks_table.put_item(
        Item={
            'student_id': regno,     # Matches your partition key
            'class': class_,         # Matches your sort key
            'math': math,
            'english': english,
            'science': science,
            'computer': computer
        }
    )

    return f"<h3>Student {name}'s data submitted to DynamoDB successfully!</h3>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
