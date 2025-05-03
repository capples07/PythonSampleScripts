# Purpose of this script is to pull all licensed users via the Smartsheet API and calculate the different between their last login date and today
# Install the smartsheet sdk with the command: pip install smartsheet-python-sdk
import smartsheet
import logging
import os
from datetime import datetime
import csv


_dir = os.path.dirname(os.path.abspath(__file__))
#dev
os.environ['SMARTSHEET_ACCESS_TOKEN'] = 'DEV_TOKEN_HERE'


print("Starting ...")

# Initialize client. Uses the API token in the environment variable "SMARTSHEET_ACCESS_TOKEN"
#dev/base site
smart = smartsheet.Smartsheet()

# Make sure we don't miss any error
smart.errors_as_exceptions(True)

# Log all calls
logging.basicConfig(filename='rwsheet.log', level=logging.INFO)

# write output to local csv
file_name = 'inactive_users.csv'
with open(file_name, mode='w', newline='') as file:
    writer = csv.writer(file)
    header = ['email','lastlogin','days']
    writer.writerow(header)
#calling api to get list of users, must specify to include lastlogin, outputting to dictionary format to iterate through
    response = smart.Users.list_users(include='lastLogin')
    users = response.to_dict()
#need to iterate through total pages because data is paginated and one call will not return the entire dataset
    pages = users["totalPages"]
    for i in range(1,pages + 1):
#selecting individual pages of request and the "data" key/value portion of the dict
        response = smart.Users.list_users(include='lastLogin',page=i)
        users = response.to_dict()
        usersdictlist = users["data"]
#only want users that have logged in, in an active status, and are licensed users
        for x in usersdictlist:
            if 'lastLogin' in x:
                if x['status'] == 'ACTIVE':
                    if x['licensedSheetCreator'] == True:
                        #print(x['email'],' - ',x['lastLogin'])
                        user_email = x['email']
                        lastlogindatetime = x['lastLogin']
                        lastlogindate = lastlogindatetime[:10]
                        #a = datetime.strptime('2025-03-01',"%Y-%m-%d").date()
                        a = datetime.now().date()
                        b = datetime.strptime(lastlogindate,"%Y-%m-%d").date()
                        delta = a - b
                        #if delta.days > 9:
                        #    print('y')
                        #else:
                        #    print('n')
                        writer.writerow([str(user_email),str(lastlogindate),str(delta.days)])
                        print(str(user_email),x['id'])


print(f"Data has been written to {file_name}")