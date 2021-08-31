from flask import Flask, make_response, request, jsonify
import requests
import time
import datetime
from google.cloud import bigquery
import _thread
import os
import datetime
import time

#Libraries for Calender API
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
#from datetime import datetime, timedelta

ts = time.time()
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/SreenivasGorantla/Downloads/Dialogflow CX/sreenivas-bot-b858df354d42.json"
#SCOPES = ['https://www.googleapis.com/auth/calendar']
#CREDENTIALS_FILE = 'C:/Users/SreenivasGorantla/Downloads/Dialogflow CX/client_secret_20925083630-as6g5mobqnrn019l52bha41vln62uv1l.apps.googleusercontent.com.json'

client = bigquery.Client()

app = Flask(__name__)

def results():
    req = request.get_json(force=True)
    print(req)
    name = req.get('fulfillmentInfo').get('tag')   
    if name == "world":
        
        url = "https://coronavirus-monitor.p.rapidapi.com/coronavirus/worldstat.php"

        headers = {
            'x-rapidapi-host': "coronavirus-monitor.p.rapidapi.com",
            'x-rapidapi-key': "ed82524e9fmsh1763d34ca4333f4p186c05jsnacbb4da30812"
            }

        response = requests.request("GET", url, headers=headers)

        world = response.json()
        
        return {
            "fulfillment_response": {
                "messages": [{
                    "text": {
                        "text": [
                            "Total Cases are : "+world['total_cases']+", New Cases are : "+world['new_cases']+", Total Deaths are : "+world['total_deaths']+ ", Total Recovered are : "+world['total_recovered']+", New Deaths are : "+world['new_deaths']
                            
                        ]
                    }
                }],
                "merge_behavior": "REPLACE"
            },
            'sessionInfo': request.json['sessionInfo']
        }
 
    elif name == "country":  
        Country = req.get('sessionInfo').get('parameters').get('country')
        #print(name)
        #print(Country)
        if Country == "united states" or Country == "US" or Country == "america" or Country == "America" or Country == "us" or Country == "United States" or Country == "U.S" or Country == "u.s." or Country == "U.S.A":
                Country = "USA"
        elif Country == "united kingdom" or Country == "uk" or Country == "britan" or Country == "United Kingdom" or Country == "Britan":
                Country = "UK"

        url = "https://coronavirus-monitor.p.rapidapi.com/coronavirus/latest_stat_by_country.php"

        querystring = {"country":Country}

        headers = {
            'x-rapidapi-host': "coronavirus-monitor.p.rapidapi.com",
            'x-rapidapi-key': "ed82524e9fmsh1763d34ca4333f4p186c05jsnacbb4da30812"
            }

        response = requests.request("GET", url, headers=headers, params=querystring)

        stat = response.json()

        stats = stat["latest_stat_by_country"][0]
        
        Country_Name = stats['country_name']
        Cases_per_1M = stats['total_cases_per1m']
        Deaths_per_1M = stats['deaths_per1m']
        Total_Tests = stats['total_tests']
        
        Total_Cases = stats['total_cases']
        New_Cases = stats['new_cases']
        Active_Cases = stats['active_cases']
        Total_Deaths = stats['total_deaths']
        Total_Recovered = stats['total_recovered']
        New_Deaths = stats['new_deaths']
        
        if Total_Cases == '':
            Total_Cases = '0'
        if New_Cases == '':
            New_Cases = '0'
        if Active_Cases == '':
            Active_Cases = '0'
        if Total_Deaths == '':
            Total_Deaths = '0'
        if Total_Recovered == '':
            Total_Recovered = '0'
        if New_Deaths == '':
            New_Deaths = '0'
        if Total_Tests == '':
            Total_Tests = '0'
                   
        
        return {
            "fulfillment_response": {
                "messages": [{
                    "text": {
                        "text": [
                            "Total Cases are : "+Total_Cases+", New Cases are : "+New_Cases+", Active Cases are : "+Active_Cases+", Total Deaths are : "+Total_Deaths+", Total Recovered are : "+Total_Recovered+", New Deaths are : "+New_Deaths+", Total Tests done are : "+Total_Tests
                            
                        ]
                    }
                }],
                "merge_behavior": "REPLACE"
            },
            'sessionInfo': request.json['sessionInfo']
        }
    
    elif name == "user_slot_info":  
        age = int(req.get('sessionInfo').get('parameters').get('age'))
        email = req.get('sessionInfo').get('parameters').get('email')
        mobile = int(req.get('sessionInfo').get('parameters').get('number-integer'))
        dose = int(req.get('sessionInfo').get('parameters').get('ordinal'))
        name = req.get('sessionInfo').get('parameters').get('person').get('name')
        if dose == 1:
            query = """SELECT * FROM `sreenivas-bot.Nancy.User_Slot_Info` where Mobile = {} and Email = '{}'""" .format(mobile, email)
            resp = client.query(query,project="sreenivas-bot").to_dataframe()
            timestamp1 = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            #slot1_date = resp.Dose_1_Slot [0]
            if resp.size == 0: 
                def bqwrite():
                    #if resp.Dose_1_Slot [0] == None:
                    #    query = """update `sreenivas-bot.Nancy.User_Slot_Info` SET Dose_1_Slot = '{}' where Mobile = {}""".format( mobile)      
                    query = """insert into `sreenivas-bot.Nancy.User_Slot_Info` (Name, Mobile, Email, Timestamp) values ('{}', {}, '{}', '{}')""".format(name, mobile, email, timestamp1)
                    client.query(query,project="sreenivas-bot")
                _thread.start_new_thread(bqwrite, ())
                result = "On which date you are looking to take your vaccine? Please say in the format of month-date-year." 
            elif resp.Dose_1_Slot [0] != None:
                result = "An appointment is already scheduled with the given details on "+str(resp.Dose_1_Slot [0])[0:-9]
            elif resp.Dose_1_Slot [0] == None:
                result = "On which date you are looking to take your vaccine? Please say in the format of month-date-year."

        if dose == 2:
            query = """SELECT * FROM `sreenivas-bot.Nancy.User_Slot_Info` where Mobile = {} and Email = '{}'""" .format(mobile, email)
            resp = client.query(query,project="sreenivas-bot").to_dataframe()
            timestamp1 = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            #slot1_date = resp.Dose_1_Slot [0]
            if resp.size == 0: 
                def bqwrite():
                    #if resp.Dose_1_Slot [0] == None:
                    #    query = """update `sreenivas-bot.Nancy.User_Slot_Info` SET Dose_1_Slot = '{}' where Mobile = {}""".format( mobile)      
                    query = """insert into `sreenivas-bot.Nancy.User_Slot_Info` (Name, Mobile, Email, Timestamp) values ('{}', {}, '{}', '{}')""".format(name, mobile, email, timestamp1)
                    client.query(query,project="sreenivas-bot")
                _thread.start_new_thread(bqwrite, ())
                result = "You haven't taken your first dose. On which date you are looking to take your first vaccine dose? Please say in the format of month-date-year." 
            elif resp.Dose_1_Slot [0] != None:
                result = "Your first dose of vaccination is done on "+str(resp.Dose_1_Slot [0])[0:-9]+". On which date you are looking to take your second vaccine dose? Please say in the format of month-date-year." 
            elif resp.Dose_1_Slot [0] == None:
                result = "You haven't taken your first dose. On which date you are looking to take your first vaccine dose? Please say in the format of month-date-year." 

                
        return {
            "fulfillment_response": {
                "messages": [{
                    "text": {
                        "text": [
                             result            
                        ]
                    }
                }],
                "merge_behavior": "REPLACE"
            },
            'sessionInfo': request.json['sessionInfo']
        }
        
 
    elif name == "dose1_date":  
        year = str(int(req.get('sessionInfo').get('parameters').get('date').get('year')))
        month = str(int(req.get('sessionInfo').get('parameters').get('date').get('month')))
        day = str(int(req.get('sessionInfo').get('parameters').get('date').get('day')))
        
        return {
            "fulfillment_response": {
                "messages": [{
                    "text": {
                        "text": [
                            "Suggested vaccination slots on " + month + " - "+ day +" - "+ year + " are : 10 AM - 12 PM, 12 PM - 2 PM and 2 PM - 4 PM. Please select a slot."              ]
                    }
                }],
                "merge_behavior": "REPLACE"
            },
            'sessionInfo': request.json['sessionInfo']
        }
    
    elif name == "dose1_time":
        year = int(req.get('sessionInfo').get('parameters').get('date').get('year'))
        month = int(req.get('sessionInfo').get('parameters').get('date').get('month'))
        day = int(req.get('sessionInfo').get('parameters').get('date').get('day'))
        hours = int(req.get('sessionInfo').get('parameters').get('date-time').get('hours'))
        minutes = int(req.get('sessionInfo').get('parameters').get('date-time').get('minutes'))
        name = req.get('sessionInfo').get('parameters').get('person').get('name')
        mobile = int(req.get('sessionInfo').get('parameters').get('number-integer'))
        email = req.get('sessionInfo').get('parameters').get('email')
        dose = int(req.get('sessionInfo').get('parameters').get('ordinal'))
        date_obj = datetime.datetime(year, month, day,hours,minutes,0)
        slot_start = str(date_obj).replace(' ','T')
        slot_end = str(date_obj + datetime.timedelta(minutes=30)).replace(' ','T')
        
        m = str(minutes)
        if len(m) == 1:
            m = '0'+m
        
        def get_calendar_service():
            creds = None
            # The file token.pickle stores the user's access and refresh tokens, and is
            # created automatically when the authorization flow completes for the first
            # time.
            if os.path.exists('token.pickle'):
               with open('token.pickle', 'rb') as token:
                   creds = pickle.load(token)
            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
               if creds and creds.expired and creds.refresh_token:
                   creds.refresh(Request())
               else:
                   flow = InstalledAppFlow.from_client_secrets_file(
                       CREDENTIALS_FILE, SCOPES)
                   creds = flow.run_local_server(port=0)

               # Save the credentials for the next run
               with open('token.pickle', 'wb') as token:
                   pickle.dump(creds, token)

            service = build('calendar', 'v3', credentials=creds)
            return service
        
        def create_event():
            print('event booking initiated')
            # creates one hour event tomorrow 10 AM IST
            service = get_calendar_service()
            event_result = service.events().insert(calendarId='c_8nk2et9htcie3gnktfvfltj6ck@group.calendar.google.com',
            body={
               "summary": 'Vaccination-slot-'+name,
               "description": 'Vaccination slot booked via chat app',
               "start": {"dateTime": slot_start, "timeZone": 'Asia/Kolkata'},
               "end": {"dateTime": slot_end, "timeZone": 'Asia/Kolkata'},
               }
            ).execute()
            print(event_result)
        query = """SELECT * FROM `sreenivas-bot.Nancy.User_Slot_Info` where Mobile = {} and Email = '{}'""" .format(mobile, email)
        resp = client.query(query,project="sreenivas-bot").to_dataframe()
        if resp.Dose_1_Slot [0] == None:        
            def bqwrite():
                #if resp.Dose_1_Slot [0] == None:
                query = """update `sreenivas-bot.Nancy.User_Slot_Info` SET Dose_1_Slot = '{}' where Mobile = {}""".format(str(date_obj), mobile)      
                client.query(query,project="sreenivas-bot")
            _thread.start_new_thread(bqwrite, ())
        else:          
            def bqwrite1():
                #if resp.Dose_1_Slot [0] == None:
                query = """update `sreenivas-bot.Nancy.User_Slot_Info` SET Dose_2_Slot = '{}' where Mobile = {}""".format(str(date_obj), mobile)      
                client.query(query,project="sreenivas-bot")
            _thread.start_new_thread(bqwrite1, ())
        result = "Your vaccination slot is booked on " + str(month) + " - "+ str(day) +" - "+ str(year) + " at " + str(hours) + ":"+ m + ". Please reach vaccination center 30 minutes prior to your slot time."
        return {
            "fulfillment_response": {
                "messages": [{
                    "text": {
                        "text": [
                            result
                        ]
                    }
                }],
                "merge_behavior": "REPLACE"
            },
            'sessionInfo': request.json['sessionInfo']
        }
       
    
@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    # return response
    return jsonify(results())


if __name__ == '__main__':
    app.run()  
