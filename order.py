#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import json

def get_mission_token(endpoint, lgcsusername, lgcspassw):
    gmt_cmd = "curl.exe -X POST \""+endpoint+"/config/missiontoken\" -k -H \"accept: application/json\" -H \"content-type: application/x-www-form-urlencoded\" -d \"lgcs_username="+lgcsusername+"&password="+lgcspassw+"\""
    print gmt_cmd
    readout = subprocess.Popen(gmt_cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, bufsize=1).communicate()[0]
    print readout.split(":")[2].replace("}","").replace("\"","").strip()
    result = readout.split(":")[1].split(",")[0].strip()
    message = readout.split(":")[2].replace("}","").replace("\"","").strip()
    return message, result


def order(wspath, missiontoken, message):
    # cmd = "C:\\Program Files (x86)\\curl-7.57.0\\src\\curl.exe -X POST \""+wspath+"\" -k -H \"accept: application/json\" -H \"Authorization: Bearer "+missiontoken+"\" -H \"content-type: application/json\" -d \""+message+"\""
    cmd = "curl.exe -k -X POST \""+wspath+"\" -H \"accept: application/json\" -H \"Authorization: Bearer "+missiontoken+"\" -H \"content-type: application/json\" -d \""+message+"\""
    # cmd = "curl -X POST \"" + wspath + "\" -H \"accept: application/json\" -H \"Authorization: Bearer " + missiontoken + "\" -H \"content-type: application/json\" -d \"" + message + " -F \"file=@C:\\Users\\Public\\rpasdc\\test_data\\HOME_W201.png\"\""
    print cmd
    readout = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, bufsize=1).communicate()[0]
    d = json.loads(readout)
    print readout.split(":")
    result = str(d['success'])
    # print readout.split(":")[2].replace("}","").replace("\"","").strip()
    # result = readout.split(":")[1].split(",")[0].strip()
    message = readout.split(":")[2].replace("}","").replace("\"","").strip()
    return result, message

http://archive.eumetsat.int/usc/#co:;id=EO:EUM:DAT:MSG:HRSEVIRI;delm=O;form=HRITTAR;band=1,2,3,4,5,6,7,8,9,10,11,12;subl=1,1,3712,3712;comp=GZIP;med=NET;noti=1;satellite=MSG4,MSG2,MSG1,MSG3;ssbt=2018-06-12T00:00;ssst=2018-06-12T01:00;udsp=OPE;subSat=0;qqov=ALL;seev=0;smod=ALTHRV