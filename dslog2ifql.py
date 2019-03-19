from dslog2csv import *
from influxdb import InfluxDBClient

import datetime

client = InfluxDBClient(host='localhost', port=8086)

def process_all_events(dir):
    import os 
    out = {}
    
    for file in os.listdir(dir):
        #print("Checking "+file)
        if ".dsevents" in file:
            #print(file)
            val = event_header(file)
            if val != None:
                out[file] = val 
    return out
    
def process_all_files(dir):
    import os 
    
    out = {}
    
    for file in os.listdir(dir):
        if ".dslog" in file:
            process_file(file)
            
def process_all_event_files(dir):
    e = process_all_events(dir)
    
    for file in e.keys():
        file_handle = file.split('.')[0]
        filename = str(file_handle+".dslog")
        process_file(filename)
    
    
def event_header(event_file):
    ev = open(event_file,'r')
    e  = list(filter(None, ev.read().split('\x00')))
    
    
    for line in e:
        if "CFMS Connected" in line:

            header = line.split(':',1)[1].split(',')
            match = header[0].strip().replace(' ','_')
            time  =  header[1].lstrip()[:-1].split(':',1)[1].strip().replace(' ','_')
            print("F: "+event_file+" | H: "+match+" | "+time)            
            ev.close()
            return match,time
            
def process_file(input_file):
    print("processing "+input_file)
    file_name = input_file.split('.')[0]
    output_file = str(file_name+'.lineproto')
    event_file = str(file_name+'.dsevents')
    dslog_file = input_file
    
    try:
        event,fms_time = event_header(event_file)
    except:
        event = "null"
        fms_time = "null"
       
    #af = open(str(output_file),'w')    #one file per log
    af = open("metrics.lineproto",'a')  #one big file

    
    dsparser = DSLogParser(dslog_file)


    col = ['time_of_day',]

    col.extend(DSLogParser.OUTPUT_COLUMNS)

    fn = file_name.split(' ')

    raw_date = fn[0]
    raw_date = raw_date.split('_')
    year = int(raw_date[0])
    month = int(raw_date[1])
    day = int(raw_date[2])

    raw_time = fn[1]
    raw_time = raw_time.split('_')
    hour = int(raw_time[0])
    minute = int(raw_time[1])
    second = int(raw_time[2])

    start_time = datetime.datetime(year, month, day, hour, minute, second)

    #print(start_time+'.')

    epoch = datetime.datetime.utcfromtimestamp(0)

    def unix_time_nanos(dt):
        return (dt - epoch).total_seconds() * 1000000

    for rec in dsparser.read_records():


         #rec['time_of_day'] = '01-01-1970-00-00-00'
         rec['time_of_day'] = str(start_time)
         for i in range(16):
             rec['pdp_{}'.format(i)] = rec['pdp_currents'][i]

         td = datetime.timedelta(0,0,0,rec['time'])
         t = start_time+td

         t = unix_time_nanos(t)
         t = str(str(int(t))+'000')

         line = (\
         'robo-pdp,'+
         #'filetime='+rec['time_of_day']+\
         'team=3985'+
         ',event='+str(event)+
         ',fms_time='+str(fms_time)+
         ' robot_auto='+str(int(rec['robot_auto']))+
         ',robot_disabled='+str(int(rec['robot_disabled']))+
         ',robot_tele='+str(int(rec['robot_tele']))+
         ',ds_tele='+str(int(rec['ds_tele']))+
         ',ds_disabled='+str(int(rec['robot_auto']))+
         ',brownout='+str(int(rec['brownout'])) +
         ',watchdog='+str(int(rec['watchdog'])) +
         ',packet_loss='+str(rec['packet_loss'])+
         ',pdp_temp='+str(rec['pdp_temp'])+
         ',pdp_voltage='+str(rec['pdp_voltage'])+
         ',pdp_id='+str(rec['pdp_id'])+
         ',bandwidth='+str(rec['bandwidth'])+
         ',wifi_db='+str(rec['wifi_db'])+
         ',rio_cpu='+str(rec['rio_cpu'])+
         ',pdp_total_current='+str(rec['pdp_total_current'])+
         ',round_trip_time='+str(rec['round_trip_time'])+
         ',pdp_resistance='+str(rec['pdp_resistance'])+
         ',ds_auto='+str(int(rec['ds_auto']))+
         ',time='+str(rec['time'])+
         ',can_usage='+str(rec['can_usage'])+
         ',voltage='+str(rec['voltage'])+
         ',pdp_0='+str(rec['pdp_0'])+
         ',pdp_1='+str(rec['pdp_1'])+
         ',pdp_2='+str(rec['pdp_2'])+
         ',pdp_3='+str(rec['pdp_3'])+
         ',pdp_4='+str(rec['pdp_4'])+
         ',pdp_5='+str(rec['pdp_5'])+
         ',pdp_6='+str(rec['pdp_6'])+
         ',pdp_7='+str(rec['pdp_7'])+
         ',pdp_8='+str(rec['pdp_8'])+
         ',pdp_9='+str(rec['pdp_9'])+
         ',pdp_10='+str(rec['pdp_10'])+
         ',pdp_11='+str(rec['pdp_11'])+
         ',pdp_12='+str(rec['pdp_12'])+
         ',pdp_13='+str(rec['pdp_13'])+
         ',pdp_14='+str(rec['pdp_14'])+
         ',pdp_15='+str(rec['pdp_15'])+
         ' '+t+'\n'
         )
         line(outline,protocol='line')
         #af.write(line)
    #af.close()
