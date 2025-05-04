#v5 changes - 20220413
#strip characters from file ext
#strip nonascii characters from s3 metadata file_name
#v4 changes - 20220412
#remove special characters instead of remapping, only keep alpha numeric in file_name
#correct rsplit indexing
#change logic to write timestamp for all uploads

import psycopg2
import boto3
import datetime
import time

def datetime_to_ms_epoch(dt):
    microseconds = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
    return str(int(round(microseconds / float(1000))))

def strip_non_alphanum(string):
    #Returns the string with only alpha numerics
    stripped = ''.join([s for s in string if s in
              'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890'])
    return stripped

def remove_non_ascii(text):
    #Strips all non-ascii characters for s3 metadata
    return ''.join(i for i in text if ord(i)<128)

def main():

    s3 = boto3.client('s3')

    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start Time =", current_time)


    # Connect to the attachment database
    connection = psycopg2.connect(user="username",
                                password="pw",
                                host="POSTGRES_DB_SERVER",
                                port="5432",
                                database="DB")

    cursor = connection.cursor()

    #pull id list, filter to run on multiple machines, limit for testing purposes
    record_list = """select id from db.table where id <= 35000 limit 1000"""
    cursor.execute(record_list)
    record_list_result = cursor.fetchall()

    upload_result = {}
    fail_list = {}

    #write to txt file on success/fail, research failures and update code to address
    try:
        with open('attachment_success.txt','r+') as f:
            successlist = [line.rstrip('\n') for line in f]
    except IOError:
        successlist=[]
    else:
        with open('attachment_success.txt','r+') as f:
            successlist = [line.rstrip() for line in f]


    for x,recordid in enumerate(record_list_result):
        tempid = str(recordid[0])
        #check if id has already been loaded and skip if it has
        if tempid in successlist:
            print("Already loaded, skipping ID = " + tempid)
            continue

        #print progress every 100 records to monitor load
        if (x % 100 == 0):
            print("Load Progress: Attachment #" + str(x) )

        try:
        #pull individual records, store columns in vars for writing to s3 metadata
            select_query = """select * from db.table where id =  """ + str(recordid[0])
            cursor.execute(select_query)
            results=cursor.fetchall()

            #parse out results to vars
            for row in results:
                id = str(row[0])
                rfc_id = str(row[1])
                attachment = str(row[2])
                file_type = str(row[3])
                file_name = str(row[4])
                #need to strip out nonascii chars to properly load into s3
                file_name = remove_non_ascii(file_name)
                legacy_file_ind = str(row[5])
                created_date = str(row[6])
                created_by = str(row[8])
                if '.' not in file_name:
                    tempfilename = file_name
                    tempext = ''
                else:
                    tempfilename = file_name.rsplit('.', 1)[0]
                    tempext = file_name.rsplit('.', 1)[-1]
                    tempext = strip_non_alphanum(tempext)
                tempfilename=strip_non_alphanum(tempfilename)
                s3filename = rfc_id + '_' + tempfilename + '-' + str(datetime_to_ms_epoch(datetime.datetime.now())) + '.' + tempext

                
            s3result=s3.put_object(Body=attachment,Bucket='s3_bucket_name',Key=s3filename,
            Metadata={'id':id,'rfc_id':rfc_id,'file_type':file_type,'file_name':file_name,'legacy_file_ind':legacy_file_ind,'created_date':created_date,'created_by':created_by})

            #if http response code is 200 then write to success file and to SQL script file for updating DB, otherwise write to fail file
            if s3result['ResponseMetadata']['HTTPStatusCode'] == 200 :
                upload_result[str(recordid[0])] = s3filename
                with open('attachment_success.txt','a+') as f:
                    f.write(str(recordid[0]) + '\n')
                with open('s3_file_name_update.sql','a+') as f:
                    f.write('UPDATE db.table SET s3_file_name = ' + "'" + s3filename + "'" + ' WHERE id = ' + id + ';\n')    
            else:
                fail_list[str(recordid[0])] = 'Fail'
                with open('attachment_fail.txt','a+') as f:
                    f.write(str(recordid[0]) + '\n')
        except:
            with open('attachment_fail.txt','a+') as f:
                f.write(str(tempid) + '\n')


    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("End Time =", current_time)

if __name__ == "__main__":
    main()