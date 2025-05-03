#Purpose of script is to read PDF data for different document types and parse out certain values to create XML meta data for upload to S3
import os
import PyPDF2
from PyPDF2 import PdfReader
import psycopg2
#from coalesce import coalesce
import glob
import sys
import shutil
import re


def main (argv):
    
    if len(sys.argv) < 4:
        print('Need three arguments!')
        sys.exit()
    
    #source_dir = c:/drs/sourcefiles
    #output_dir = c:/drs/outputfiles
    #example syntax to call script
    #python drs_info_safo.py c:/drs/sourcefiles all c:/drs/outputfiles
    
    file_dir = sys.argv[1]   
    file_type = sys.argv[2]   ### values: info, safo, all
    xml_file_dir = sys.argv[3]  #output xml file dir
    
    if not (file_type == 'info' or file_type == 'safo' or file_type == 'all'):
        print('File type is wrong!')
        sys.exit()

 #connection credentials   
    s_host = 'DEV_POSTGRES_SERVER' 
    s_port = 5432
    s_db = 'postgres'
    s_user = 'user'
    passwrd = 'pass'
    
    #### Establish connection to DB
    connection = db_connection(s_host,s_port,s_db,s_user,passwrd)
    connection.set_isolation_level(0)
    
    print(file_type)
    
    if file_type == 'all':
    
        read_pdf_files_to_db(file_dir, 'info', connection)
        
        read_pdf_files_to_db(file_dir, 'safo', connection)
    else:
        read_pdf_files_to_db(file_dir, file_type, connection)
               
 #This stored procedure populates fields extracted from file content               
    run_stored_procedure(connection)  
    
    if file_type == 'all':
        generate_meta_xml_files('info', xml_file_dir, connection)
        generate_meta_xml_files('safo', xml_file_dir, connection)
        generate_output_pdf_files('info', xml_file_dir, connection)
        generate_output_pdf_files('safo', xml_file_dir, connection)
    else:
        generate_meta_xml_files(file_type, xml_file_dir, connection)
        generate_output_pdf_files(file_type, xml_file_dir, connection)
                
    connection.close()
    
def read_pdf_files_to_db(p_file_dir, p_file_type, p_connection):  
    
    delete_data_stag_table(p_connection, p_file_type)
    
    fullpath = p_file_dir + '/'+ p_file_type 
    
    print(fullpath)
    
    for fullname in glob.iglob(fullpath + '/' + '**/*.pdf', recursive=True):
         
        print(fullname)
        
        #removing path from file name to store in db
        file_name_year = fullname.replace(fullpath, '')
        print(file_name_year)
        
        #year subfolder
        #simple_file_name = file_name_year[6:]
        
        #no year subfolders
        simple_file_name = file_name_year[1:]
        
        print(simple_file_name)
        
        #function to extract pdf text
        pdfproperty = get_info(fullname)
                
        #add to db table
        pop_db_table(p_file_type, fullname, simple_file_name, pdfproperty, p_connection)
   
    
def generate_output_pdf_files(p_file_type, p_output_file_dir, p_connection):
    
    
    cursor = p_connection.cursor()
    if p_file_type == 'info':
        sql = "select full_file_name, output_pdf_file_name from data_migration_info_safo.info_safo_pdf_data where  file_type = 'info' and output_pdf_file_name is not null"
    else:
        sql = "select full_file_name, output_pdf_file_name from data_migration_info_safo.info_safo_pdf_data where  file_type = 'safo' and output_pdf_file_name is not null"
          
    cursor.execute(sql)

    sql_results = cursor.fetchall()
    
    for row in sql_results:
    
        full_source_file = row[0]
        target_file = row[1]
        
        final_tgt_file = p_output_file_dir + '/'+ p_file_type + '/' + target_file
        
        print (final_tgt_file)
        shutil.copy(full_source_file,final_tgt_file)
    
    cursor.close() 
    
def generate_meta_xml_files(p_file_type, p_output_file_dir, p_connection):
    
    
    cursor = p_connection.cursor()
    if p_file_type == 'info':
        sql = "select xml_metadata_file_name, xml_file_content from data_migration_info_safo.info_safo_pdf_data where  file_type = 'info' and xml_file_content is not null and xml_metadata_file_name is not null"
    else:
        sql = "select xml_metadata_file_name, xml_file_content from data_migration_info_safo.info_safo_pdf_data where  file_type = 'safo' and xml_file_content is not null and xml_metadata_file_name is not null"
          
    cursor.execute(sql)

    sql_results = cursor.fetchall()
    for row in sql_results:
    
        xml_meta_file_name = row[0]
        xml_file_content = row[1]
        
        full_path = p_output_file_dir + '/' + p_file_type + '/' + xml_meta_file_name 
        print(full_path)
        
        #print(full_path)
        #length = len(wk_list)
        length = len(xml_file_content)
        
        if length > 0:
            ##Check directory existence
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            
            f = open(full_path, 'w')  # open file in write mode
            f.write(xml_file_content)
    
    cursor.close() 
    
def pop_db_table(p_file_type, p_full_filename, p_simple_file_name, p_properylist, p_connection):
    
    
    author = p_properylist[0]
    title = p_properylist[1]
    subject = p_properylist[2]
    #fullcontent = p_properylist[3]
    #adding regex to replace spaces in Subject: and Purpose: part of file content, then remove nonascii chars
    fullcontent = re.sub(r'S\s*u\s*b\s*j\s*e\s*c\s*t\s*:', 'Subject:', p_properylist[3])    
    fullcontent = re.sub(r'P\s*u\s*r\s*p\s*o\s*s\s*e\s*:', 'Purpose:', fullcontent)
    fullcontent = re.sub(r'O\s*P\s*R\s*:', 'OPR:', fullcontent)
    fullcontent = re.sub(r'[^\x00-\x7F]+',' ', fullcontent)
    
    cursor = p_connection.cursor()
    sql = "insert into  data_migration_info_safo.info_safo_pdf_data (file_type, full_file_name, simple_file_name, author, file_property_title, file_property_subject, file_content_raw )\
          values(%s,%s,%s,%s,%s,%s,%s) "
    
    record_to_insert = (p_file_type, p_full_filename, p_simple_file_name, author, title, subject, fullcontent)
   
    cursor.execute(sql, record_to_insert)

    cursor.close()
    
def delete_data_stag_table( p_connection, p_file_type):
    
    if p_file_type == 'info':
        sql = "delete from data_migration_info_safo.info_safo_pdf_data where file_type = 'info'"
    else: 
        sql = "delete from data_migration_info_safo.info_safo_pdf_data where file_type = 'safo'"
 
    
    cursor = p_connection.cursor()
    
    cursor.execute(sql)
    
    cursor.close()    
    
def run_stored_procedure( p_connection):
    
    
    cursor = p_connection.cursor()
    #process data in postgres (parsing fields, removing special characters)
    sql = 'call data_migration_info_safo.process_data()'
    
    cursor.execute(sql)
    #proess meta data and create xml template in postgres
    sql = 'call data_migration_info_safo.xml_meta_file_content_process()'
    
    cursor.execute(sql)
    
    cursor.close()
    
def get_info(path):
#extract pdf meta data and full pdf text data for further parsing
    file_content_list = []
    
    pdfFileObject = open(path, 'rb')
 
    pdfReader = PyPDF2.PdfReader(pdfFileObject)
    
    info = pdfReader.metadata
    author = info.author
    creator = info.creator
    producer = info.producer
    subject = info.subject
    title = info.title
 
    #print(" ------- No. Of Pages :", pdfReader.numPages)
 
    i = 0
    
    #while i < pdfReader.numPages:
    while i < len(pdfReader.pages):
    
        #pageObject = pdfReader.getPage(i)
        pageObject = pdfReader.pages[i]
        
        #file_content_list.append(pageObject.extractText())
        file_content_list.append(pageObject.extract_text())
        
        i = i + 1
        
    #union all text data together
    full_file_content = ''.join(file_content_list)
    
    #print(full_file_content)
 
    
    pdfFileObject.close()
        
    return [author, title, subject, full_file_content]

def db_connection(s_host,s_port,s_db,s_user,passwrd):
    conn = psycopg2.connect(
    host=s_host,
    user=s_user, 
    port=s_port, 
    password=passwrd, 
    dbname=s_db,
    sslmode='require')
    return conn
    

                
if __name__ == "__main__":
    main(sys.argv[1:])
