
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 10:33:06 2024

@author: anwar
"""
import functions_framework
import requests
import gspread
import re
from datetime import datetime, timedelta
from google.cloud import bigquery
#import warnings
#warnings.filterwarnings("ignore")

############################################################################
class Base:
  message_template_id = ''
  testing = True
  debug   = True

#----------------------------------------------------------------------------
  def __init__(self, message_template_id=''):
    if message_template_id:
      self.message_template_id = message_template_id

#----------------------------------------------------------------------------
  def get_patient_mobile_from_telecom(self, telecom=''):
    mobile  = ''
    mobile2 = ''
    if telecom:
      rtelecom = re.search(r'^\s*(\d+)(\s*[|,; ]\s*(\d+))?\s*$', telecom)
      if rtelecom: 
        mobile  = rtelecom.group(1)
        mobile2 = rtelecom.group(3)
        return mobile, mobile2

    return mobile, mobile2

#----------------------------------------------------------------------------
  def get_server_name_from_source(self, source):
    rsource = re.search(r'^https://([^\.]+)\.sid-indonesia.org', source)
    if rsource:
      return rsource.group(1)
    else:
      return source


############################################################################
class GoogleSheet(Base):
  gs_report_name = "FHIR WA Report"
  gs_report_id   = ""
  token_filename = ""
  
#----------------------------------------------------------------------------
  def __init__(self, sheet_name):
    gc = gspread.service_account(filename=self.token_filename)
    sh = gc.open_by_key(self.gs_report_id)
    
    self.sheet_name = sheet_name

    self.worksheet_list = sh.worksheets()
    self.worksheet      = sh.worksheet(self.sheet_name)
        
    self.gs_report_list = []
    
#----------------------------------------------------------------------------
  def create_report_list(self, server_name, id, identifier, no_hp, name, puskesmas, city, last_mens_date, last_visit_date, gestational_age, trimester, status="Not Executed"):
    now = datetime.now()
    date_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    self.report_list = [date_time_str, server_name, id, identifier, no_hp, name, puskesmas, city, last_mens_date, last_visit_date, self.next_visit_date, gestational_age, trimester, status]
    self.report_list.extend(self.add_data_for_pivot_wa(date_time_str, status))
    if self.testing: self.report_list.append('Testing')

#------------------------------------------------------------------------
  def add_data_for_pivot_wa(self, datetime_str, status):
    adate    = ''
    month    = ''
    whatsapp = ''
    rdatetime = re.search(r'^((\d{4}-\d{2})-\d{2})', datetime_str)
    if rdatetime:
      adate = rdatetime.group(1)
      month = rdatetime.group(2) +'-01'
  
    if status == 200:
      whatsapp = 'Success'
    elif status == 500:
      whatsapp = 'Failed'
    else:
      whatsapp = status

    return [adate, month, whatsapp]

    
#==========================================================================
class Qontak(Base):
  wa_authorization          = ""
  wa_channel_integration_id = ""
  hp_test_no = ""   # nomor pribadi untuk testing

#----------------------------------------------------------------------------
  def __init__(self, message_template_id=''):
    Base.__init__(self, message_template_id)
    self.qontak_wa_report_log = []
    
#----------------------------------------------------------------------------
  def collect_wa_log_report(self):
    # Prepare headers with bearer token
    headers = {
      "Authorization": self.wa_authorization
    }
    
    response = requests.get("https://service-chat.qontak.com/api/open/v1/broadcasts/whatsapp?direction=desc", headers=headers)
  
    if response.status_code != 200:
      return {}
  
    response_json = response.json()
  
    if 'data' not in response_json:
      return {'data': [ ]}
    
    return response_json  
    
#----------------------------------------------------------------------------
  def get_wa_report_log_from_qontak(self, isend_at):
    self.qontak_wa_report_log = []
    response = self.collect_wa_log_report()
    print('Total WA log report: '+ str(len(response['data'])))
    for data in response['data']:
      rsend_at = re.search(r'^(\d{4}-\d{2}-\d{2})', data['send_at'])
      if rsend_at:
        if rsend_at.group(1) == isend_at:
          if data['message_template']['id'] == self.message_template_id:
            contact_extra = data['contact_extra']
#            contact_extra['send_at'] = isend_at
            self.qontak_wa_report_log.append(contact_extra)
              
    return self.qontak_wa_report_log
  
#----------------------------------------------------------------------------
  def check_wa_sent_from_qontak_log_by_patient_name(self, parameter, patient_name):
    for wa_sent in self.qontak_wa_report_log:
      if parameter in wa_sent:
        if wa_sent[parameter] == patient_name:
            return True
      
    return False
  
#----------------------------------------------------------------------------
  def wa_direct_send(self, mobile_number, customer_name):
    print(f"wa_direct_send('{mobile_number}', '{customer_name}')")

    if self.testing:
      mobile_number = self.hp_test_no
      customer_name  = 'Testing Ibu'      
      if mobile_number:
        return 'OK', 200
      else:
        return 'Failed', 500      
      
    if not mobile_number.startswith("62"):
      mobile_number = "62" + mobile_number
          
    data = {
      "to_name": customer_name,
      "to_number": mobile_number,
      "message_template_id": self.message_template_id,
      "channel_integration_id": self.wa_channel_integration_id,
      "language": {
        "code": "en"
      },
      "parameters": {
        "body": [
          {
            "key": "1",
            "value_text": customer_name,
            "value": "customer_name"
          },
          {
            "key": "2",
            "value_text": self.next_visit_date,
            "value": "next_visit_date"
          }
        ]
      }
    }
    
    # Prepare headers with bearer token
    headers = {
      "Authorization": self.wa_authorization
    }
    
    try:
        # Send the Qontak API Whatsapp POST request
        response = requests.post("https://service-chat.qontak.com/api/open/v1/broadcasts/whatsapp/direct", json=data, headers=headers)
        response.raise_for_status()
  
        if self.debug:
          print(f"[debug] SUCCESS phone:{mobile_number}, customer_name:{customer_name}, next_visit_date:{self.next_visit_date} sent to Qontak ANC Reminder")

        return f"Successfully sent request: {response.text}", 200
    except requests.exceptions.RequestException as e:
        if self.debug:
          print(f"[debug] ERROR phone:{mobile_number}, customer_name:{customer_name}, next_visit_date:{self.next_visit_date} exception:{str(e)}")

        return f"Error sending request: {str(e)}", response.json()['error']['code']


#==========================================================================
class BigQuery(Base):
  
#-----------------------------------------------------------------------------
  def __init__(self, query_filename, days_before_wa=4, last_visit_week_range=6, message_template_id=''):
    self.days_before_wa        = 4
    self.last_visit_week_range = 6
    
    if days_before_wa: 
      self.days_before_wa = days_before_wa

    if last_visit_week_range: 
      self.last_visit_week_range = last_visit_week_range

    Base.__init__(self, message_template_id)
    self.client = bigquery.Client()
    self.query  = ''
    
    self.read_file_and_replace_param_to_query(query_filename)
    
#----------------------------------------------------------------------------
  def read_file_and_replace_param_to_query(self, query_filename):
    fin = open(query_filename)
    query = fin.read()
    fin.close()

    query = query.replace('{self.last_visit_week_range}', str(self.last_visit_week_range))
    query = query.replace('{self.days_before_wa}', str(self.days_before_wa))
    self.query = query.replace('{self.message_template_id}', str(self.message_template_id))
      
#----------------------------------------------------------------------------
  def create_sent_id_to_bq(self, patient_id, identifier, name, mobile, status):
    if self.testing: return
    query = f"INSERT INTO `nextgen-398301.fhir_wa.sent_status` (`id`, `identifier`, `name`, `mobile_no`, `template`, `status`, `timestamp`) VALUES ('{patient_id}', '{identifier}', '{name}', '{mobile}', '{self.message_template_id}', '{status}', TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Jakarta'))) "
    query_job = self.client.query(query)

    return query_job.result()

#----------------------------------------------------------------------------
  def get_bq_wa_sent_identifier_list(self):
    query_job = self.client.query(f"SELECT identifier FROM `nextgen-398301.fhir_wa.sent_status` \
      WHERE `template` ='{self.message_template_id}' AND EXTRACT(DATE FROM `timestamp`) = CURRENT_DATE('Asia/Jakarta') ")
    rows = query_job.result()  # Waits for query to finish    
    identifier_list = []
    for row in rows:
      identifier_list.append(row.identifier)
      
    return identifier_list

#----------------------------------------------------------------------------
  def get_patient_data(self):    
    query_job = self.client.query(self.query)  # API request
    rows = query_job.result()  # Waits for query to finish

    return rows


#==========================================================================
class WA_ANC_Reminder(BigQuery, Qontak, GoogleSheet):
  
#-----------------------------------------------------------------------------
  def __init__(self, days_before_wa=4, last_visit_week_range=6):
    Qontak.__init__(self, 'bfc118d0-fd0f-4a9f-950f-e4952cda3935')
    BigQuery.__init__(self, 'anc_reminder.sql', days_before_wa, last_visit_week_range)
    GoogleSheet.__init__(self, 'anc_visit_reminder')

    self.today     = datetime.today()
    self.today_str = self.today.strftime("%Y-%m-%d")
    
    self.next_visit_date = (self.today + timedelta(days=self.days_before_wa)).strftime("%Y-%m-%d")

#----------------------------------------------------------------------------
  def test(self):
    total_counter = 0
    for row in self.get_patient_data():
      total_counter += 1
      print(f'{row.Source}|{row.Id}|{row.Identifier}|{row.Name}|{row.Telecom}|{row.District}|{row.City}|{ row.last_mens_date}|{row.gestational_age}|{row.Trimester}|{row.last_visit_date}|{row.next_visit_date}')
      
    print(f'  Total: {total_counter}')
    
    print(self.query)
    
#----------------------------------------------------------------------------
  def execute(self):
    row_num = len(self.worksheet.get_all_values())
    batch_data_list = []

    patient_rows = self.get_patient_data()
    total_counter = 0
    counter = 0
    if self.debug:
      print('[debug] {patient_data.Source}|{patient_data.Id}|{patient_data.Identifier}|{patient_data.Name}|{patient_data.Telecom}|{patient_data.District}|{patient_data.City}|{patient_data.last_mens_date}|{patient_data.gestational_age}|{patient_data.Trimester}|{patient_data.last_visit_date}|{self.next_visit_date}')
    for patient_data in patient_rows:      
      total_counter += 1
      counter += 1
      server_name     = self.get_server_name_from_source(patient_data.Source)
      patient_id      = patient_data.Id
      identifier      = patient_data.Identifier
      name            = patient_data.Name
      mobile, mobile2 = self.get_patient_mobile_from_telecom(patient_data.Telecom)
      district        = patient_data.District
      city            = patient_data.City
      last_mens_date  = patient_data.last_mens_date
      gestational_age = patient_data.gestational_age
      trimester       = patient_data.Trimester
      last_visit_date = patient_data.last_visit_date
      finalStatus     = 500
      
      if city:
        city = city.replace('_', ' ')
        city = city.capitalize()
        
      if self.debug:
        print(f'[debug] patient data: {patient_data.Source}|{patient_data.Id}|{patient_data.Identifier}|{patient_data.Name}|{patient_data.Telecom}|{patient_data.District}|{patient_data.City}|{patient_data.last_mens_date}|{patient_data.gestational_age}|{patient_data.Trimester}|{patient_data.last_visit_date}|{self.next_visit_date}')

      if mobile:
        result, finalStatus = self.wa_direct_send(mobile, name)
        self.create_report_list(server_name, patient_id, identifier, mobile, name, district, city, last_mens_date, last_visit_date, gestational_age, trimester, finalStatus)

        if finalStatus == 500 and mobile2:
          result, finalStatus = self.wa_direct_send(mobile2, name)
          self.create_report_list(server_name, patient_id, identifier, mobile2, name, district, city, last_mens_date, last_visit_date, gestational_age, trimester, finalStatus)

        if mobile2:
          mobile = mobile +' '+ mobile2
          
        if finalStatus == 200: 
          self.create_sent_id_to_bq(patient_id, identifier, name, mobile, 'sent')
        else:
          self.create_sent_id_to_bq(patient_id, identifier, name, mobile, 'failed')

      else:
        self.create_sent_id_to_bq(patient_id, identifier, name, mobile, 'NA')
        self.create_report_list(server_name, patient_id, identifier, mobile, name, district, city, last_mens_date, last_visit_date, gestational_age, trimester, 'Not Executed')

      if self.report_list:
        batch_element = []
        batch_element.append(self.report_list)
        batch_data = dict()
        row_num += 1
        batch_data['range']  = f'A{row_num}:R{row_num}'
        if self.testing: batch_data['range']  = f'A{row_num}:S{row_num}'
        batch_data['values'] = batch_element
        batch_data_list.append(batch_data)
        self.report_list = []
            
      if counter == 50:
        self.worksheet.add_rows(50)
        counter = 0
        if batch_data_list:
          self.worksheet.batch_update(batch_data_list)
          row_num = len(self.worksheet.get_all_values())
          batch_data_list = []

    print(f'  Total: {total_counter}')

    if batch_data_list:
      self.worksheet.batch_update(batch_data_list)

#===========================================================================
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def main_pubsub(cloud_event):
#if __name__ == "__main__":
  myWA_ANC_Reminder = WA_ANC_Reminder()
  myWA_ANC_Reminder.testing = False            # default is True
  myWA_ANC_Reminder.debug   = True            # default is True
  myWA_ANC_Reminder.execute()
#  myWA_ANC_Reminder.test()

### TESTING ###
#  result, finalStatus = myWA_ANC_Reminder.wa_direct_send('087876370598', 'TEST Mother')
#  print(result, finalStatus)
