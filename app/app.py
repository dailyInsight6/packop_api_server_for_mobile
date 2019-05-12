import os, sys, io
import cv2
import numpy as np
import cognitive_face as CF
import pyodbc
import json
import keymanager
import manage_data

# Imports for the REST API
from flask import Flask, request

app = Flask(__name__)
path = os.path.dirname(os.path.abspath(__file__))

# Face recognition connection info
SUBSCRIPTION_KEY = keymanager.SUBSCRIPTION_KEY
BASE_URL = keymanager.BASE_URL
# PERSON_GROUP_ID = 'pac10gix01001'
PERSON_GROUP_ID = keymanager.PERSON_GROUP_ID
CF.BaseUrl.set(BASE_URL)
CF.Key.set(SUBSCRIPTION_KEY)

# Device info
SERVER = keymanager.SQL_SERVER_ADDRESS
DATABASE = keymanager.DATABASE_NAME
USERNAME = keymanager.DATABASE_USERNAME
PASSWORD = keymanager.DATABASE_PASSWORD


@app.route('/get_history/<device_id>/<start_date>/<end_date>', methods=['GET'])
def get_history(device_id, start_date, end_date):
    con = manage_data.get_connection()
    target = ["date", "time", "stolen_yn", "video_url", "image_url"]
    condition = {"device_id": device_id}
    date_condition = [start_date, end_date]
    result = manage_data.select_data(con, "packop_transaction", target, condition, date_condition, True)
    con.close()

    return json.dumps(result)

@app.route('/get_device_info/<device_id>', methods=['GET'])
def get_device_info(device_id):
    con = manage_data.get_connection()
    target = ["first_name", "last_name", "address"]
    condition = {"device_id": device_id}
    result = manage_data.select_data(con, "member", target, condition)
    con.close()

    return json.dumps(result)

@app.route('/get_report_info/<device_id>', methods=['GET'])
def get_report_info(device_id):
    con = manage_data.get_connection()
    sql_statement = """SELECT * FROM (
                    SELECT A.DATE, A.FORECAST_VALUE, (SELECT WEEKDAYNAME FROM date where date = a.date) as WEEKDAYNAME, '' AS TOT_CNT, '' AS PROP
                    FROM theft_forecast A
                    WHERE date between CONVERT (date, SYSDATETIMEOFFSET()) and DATEADD(DAY,13,CONVERT (date, SYSDATETIMEOFFSET())) 
                    UNION ALL
                    SELECT '1000-01-01' AS DATE, '' AS FORECAST_VALUE, '' AS WEEKDAYNAME, SUM(TOT_CNT) AS TOT_CNT, SUM(CNT)*100/SUM(TOT_CNT) AS PROP FROM (
                    SELECT COUNT(*) AS CNT, ''AS TOT_CNT FROM packop_transaction
                    WHERE DEVICE_ID = '{:s}'
                    AND PIB_YN = 'y' AND STOLEN_YN = 'n'
                    GROUP BY PIB_YN, STOLEN_YN
                    UNION ALL
                    SELECT '' AS CNT, COUNT(*) AS TOT_CNT FROM packop_transaction
                    WHERE DEVICE_ID = '{:s}'
                    AND PIB_YN = 'y'
                    GROUP BY PIB_YN
                    ) A )B
                    ORDER BY DATE ASC""".format(device_id, device_id)
    result = manage_data.custom_select_data(con, sql_statement)
    con.close()

    return json.dumps(result)

@app.route('/create_person/<name>', methods=['GET', 'POST'])
def create_person(name="default"):
    full_name = name.split()
    first_name = full_name[0]
    last_name = full_name[1]
    
    data = json.loads(request.get_data())
    user_folder = os.path.sep.join([path, "images/{}".format(name)])

    if not os.path.exists(user_folder):
        os.mkdir(user_folder)
        response = CF.person.create(PERSON_GROUP_ID, name)
        person_id = response['personId']
        txt_file_path = os.path.sep.join([user_folder, "{}.txt".format("person_id")])
        file = open(txt_file_path, "w")
        file.write(person_id)
        file.close()
    else:
        txt_file_path = os.path.sep.join([user_folder, "{}.txt".format("person_id")])
        if not os.path.exists(txt_file_path):
            file = open(txt_file_path, "w")
            response = CF.person.create(PERSON_GROUP_ID, name)
            person_id = response['personId']
            file.write(person_id)
            file.close()
        else:
            file = open(txt_file_path, "r")
            person_id = file.read()
            file.close()
    
    # Insert Data 
    con = manage_data.get_connection()
    target = ["first_name", "last_name", "address"]
    condition = {"member_id": name}
    result = manage_data.select_data(con, "member", target, condition)
    if len(result) < 1:
        data_dict = [name, data["deviceId"], "none", 0, data["address"], first_name, last_name]
        manage_data.insert_data(con, "member", data_dict)
    manage_data.close_connection(con)
    return person_id


@app.route('/train_face/<person_id>/<name>/<count>', methods=['POST'])
def train_face(person_id, name="default", count="0"):
    return_value = " ok"
    # read images file string data
    filestr = request.files['photo'].read()
    # convert string data to numpy array
    npimg = np.fromstring(filestr, np.uint8)
    # convert numpy array to images
    img = cv2.imdecode(npimg, cv2.IMREAD_COLOR)
    # save the images
    user_folder = os.path.sep.join([path, "images/{}".format(name)])
    image_file_path = os.path.sep.join([user_folder, "{}.jpg".format(count)])
    # Save image file
    cv2.imwrite(image_file_path, img)

    if int(count) < 4:
        # add faces to person id
        CF.person.add_face(image_file_path, PERSON_GROUP_ID, person_id)

        if int(count) == 3:
            # train faces
            CF.person_group.train(PERSON_GROUP_ID)
            response = CF.person_group.get_status(PERSON_GROUP_ID)
            status = response['status']
            return_value = " finished"
    else:
        detected_people = CF.face.detect(image_file_path)
        face_ids = [d['faceId'] for d in detected_people]
        faces = CF.face.identify(face_ids, PERSON_GROUP_ID)
        for face in faces:
            if len(face['candidates']) > 0:
                return_value = "confirmed" 
    return return_value

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=80 )