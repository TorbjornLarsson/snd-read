#!/usr/bin/env python3

import requests
import os
import lxml.etree as ET
import glob
import json
import sys
import shutil
import csv
import pandas as pd
import regex as re
from jinja2 import Environment, FileSystemLoader
environment = Environment(loader=FileSystemLoader("."))
    
def load_xml(xpath):
# Laddar xml från snd sökning, kontrollerar att det inte finns någon felkod, och sparar response.xml i källkatalog.
    oai_url = (r'https://snd.gu.se/oai-pmh?verb=ListRecords&metadataPrefix=oai_dc&set=principal:slu.se')
    response = requests.get(oai_url).content
    e = 'error code='
    spl = str(response).split(e)
    if len(spl) > 1:
        sple = spl[1].split('<')[0]
        print(e, sple)
        sys.exit()     
    os.makedirs(xpath, exist_ok=True)
    with open(os.path.join(xpath, 'response.xml'), 'wb') as sndxml:
        sndxml.write(response)  
    # Kollar efter resumeToken.
    def token(xmlpath):
        with open(xmlpath, 'r') as XMLfile:    
            tree = ET.parse(XMLfile)
            root = tree.getroot()
        for lr in root.findall("./{http://www.openarchives.org/OAI/2.0/}ListRecords/{http://www.openarchives.org/OAI/2.0/}resumptionToken"):
            return lr.text
    lastxml = os.path.join(xpath, 'response.xml')
    xmli = 1
    lasttok = token(lastxml)
    # Om token finns skrivs den nya requesten till response.xml.
    while (lasttok is not None):
        xmli += 1
        lastxml = os.path.join(xpath, 'response_' + str(xmli) + '.xml')
        with open(lastxml, 'wb') as XMLfile_last:
            result = 'https://snd.gu.se/oai-pmh?verb=ListRecords&resumptionToken=' + lasttok
            print('Requesting more from', '\n', result, '\n')
            append_url = result
            resp_last = requests.get(append_url).content
            XMLfile_last.write(resp_last)
        lasttok = token(lastxml)
def parseXML(xpath):
    # Läser response.xml och hämtar lista på samlingar och datestamp, Skriver till fil snds_from_response.txt.
    for respxml in glob.glob(os.path.join(xpath, 'response*.xml')):
        with open(respxml, 'r') as XMLfile:
                tree = ET.parse(XMLfile)
                root = tree.getroot()
        # Separerar samlings id och datestamp med komma för att göra jämföring möjlig vid senare körningar om samlingar är nya eller uppdaterade.
        with open(os.path.join(xpath, 'list_s.txt'), 'a') as r:
            with open(os.path.join(xpath,'list_t.txt'), 'a') as x:
                for elm in root.findall("./{http://www.openarchives.org/OAI/2.0/}ListRecords/{http://www.openarchives.org/OAI/2.0/}record/{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}identifier"):
                    collection = [elm.text]
                    for line1 in collection:
                        collection_rad = (line1 + "\n")
                        r.write(collection_rad)
                for elm_2 in root.findall("./{http://www.openarchives.org/OAI/2.0/}ListRecords/{http://www.openarchives.org/OAI/2.0/}record/{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}datestamp"):
                    time = [elm_2.text]
                    for line2 in time:
                        time_row = (line2 + "\n")
                        x.write(time_row)
        # Slår ihop svaret (samling och tidslista) till snds_from_response.txt. 
        with open(os.path.join(xpath, 'list_s.txt')) as sh:
            with open(os.path.join(xpath,'list_t.txt')) as th:
                with open(os.path.join(xpath, 'snds_from_response.txt'), 'w') as sr:
                    shlines = sh.readlines()
                    thlines = th.readlines()
                    for i in range(len(thlines)):
                        line = shlines[i].strip() + ';' + thlines[i]
                        sr.write(line)

def compare_snds(path):
    # Jämför lista med samlingar från snd sökningen (snds_from_response.txt) och den som är gjord tidigare (snds.txt). 
    with open(os.path.join(path,'snds_from_response.txt')) as r:
        set_resp = set(line.strip() for line in r)
    with open('snds.txt') as o:
        set_old = set(line.strip() for line in o)
    new_set = set(set_resp.difference(set_old))
    new_list = list(new_set)
    # Tar bort tempfiler och mappar om ingen ny samling hittats.
    if not new_list:
        print('No new or updated collections')
        print('No new or updated collections added to snds.txt')
        for temp_txt in glob.glob(os.path.join(path+'/*.txt')):
            os.remove(temp_txt)
        for temp_xml in glob.glob(os.path.join(path+'/*.xml')):
            os.remove(temp_xml)
        os.removedirs(path)
        print('\n'+'Done'+'\n')
        sys.exit()()
    else:
        # Skriver skillnaden till new_snds.txt.
        with open(os.path.join(path, 'new_snds.txt'), 'a+') as new_file:
            for line in new_list:
                # Körningen 240131 har foldrar med problem med filer, de utesluts:
                if line != '2022-252-1;2023-11-16T13:02:22Z':
                    # Och en folder på gamla formatet:
                    if line != 'snd1105-1;2023-09-18T18:27:26Z':
                        new_file.write(line)
                        new_file.write('\n')
                        print(line)
            if line:
                print('Downloading new collections')
    o.close()
    with open('snds.txt', 'a') as adsnd:
        with open(os.path.join(path, 'new_snds.txt'), 'r') as apsnd:
            snd = apsnd.read().splitlines()
            print('\n'+'added to snds.txt') 
            i = 0
            for line in snd:                
                adsnd.write(line)
                adsnd.write('\n')
                i=1+i
    print('total number of collections:', i)
    with open('snds.txt', 'r') as r:
        with open('snds_s.txt', 'w') as q:
            x = r.readlines()
            x.sort()
            for line in range(len(x)):
                q.write(x[line])
    os.remove('snds.txt')
    os.rename('snds_s.txt', 'snds.txt')
def get_snds(path):
    # Hämtar metadata till nya samlingar från SND i json-ld format.
    with open(os.path.join(path, 'new_snds.txt')) as f:
        snds_1 = f.read().splitlines()
        for line in snds_1:
            line_2 = line.split(';')
            snd = line_2[0]
            # /1/ är fast tillagt tills SND förklarat sitt nya folderträd.
            jurl = 'https://snd.gu.se/sv/catalogue/dataset/' + snd + '/1/export/json-ld'
            jfname = snd + '.json-ld'
            r = requests.get(jurl)
            with open(os.path.join(path, jfname), 'w') as f:
                f.write(r.text)
            # Skapar en json-ld deriverad tempfil utan @ för enklast bibehållen mustache templating
            jfnoatname = snd + '.json-ld-noat'
            with open(os.path.join(path, jfnoatname), 'w') as f2:
                with open(os.path.join(path, jfname), 'r') as f:
                    string = f.read()
                    new_string = re.sub('@','',string)
                    f2.write(new_string)
def load_files(path_1, path_2):
    # Söker ut namn för samlingarna.
    for filename in glob.glob(os.path.join(path_1 +'/*.json-ld')):
        print(filename)
        # Läser in json-ld filen, och gör mapp för att hämta samlingen.
        with open(filename+'-noat') as f:
            dcitems = json.load(f, parse_int = str)
            base1, ext = os.path.splitext(filename)
            srcd = base1.split('/')
            base = srcd[-1]
            print(path_2)
            newdir = (os.path.join(path_2, base))
            os.makedirs(newdir, exist_ok=True)
            # Öppnar jinja underlag.
            template = environment.get_template("filename_url.jinja")
            contemplate = environment.get_template("contents.jinja")
            itemtemplate = environment.get_template("itemtitle.jinja")
            # Läser in hämtad json-ld från tempmappen, gör underlag för filhämtning.
            for orgfile in glob.glob(base1 + '.json-ld-noat'):
                with open (os.path.join(path_1, orgfile)) as f:
                    filedata = json.load(f)
                    base, ext = os.path.splitext(orgfile)
                    sndname = base.split('/')[-1]
                    print('\n'+sndname+':')
                with open(os.path.join(newdir,"" + 'itemtitle.txt'), 'w') as collection:                        
                    collection.write(itemtemplate.render(dcitems))
                # Genererar contents fil från underlag och lägger till json-ld fil.
                with open(os.path.join(newdir,"" + 'contents'), 'w') as filenames:
                    filenames.write(contemplate.render(filedata))
                with open(os.path.join(newdir,"" + 'contents'), 'a') as metadata:
                    json_data = sndname + '.json-ld'
                    metadata.write(json_data)
                # Lägger till samling i datalog.csv
                with open('snd_datalog.csv', 'a+', encoding='utf8') as datalog:
                    size = os.path.getsize('snd_datalog.csv')
                    writer = csv.writer(datalog, delimiter=";", lineterminator="\n")
                    header = csv.writer(datalog, lineterminator="\n")
                    if filedata["version"] != []:
                        version = filedata["version"]
                    else:
                        version = ('-')
                    
                    if size == 0:
                        header.writerow(['snd_id', 'modified', 'dspace', 'archive_holder', 'registration_number', 'dataset_version', 'handle', 'collection_submit_id', 'additional_information'])
                    new_snd = [sndname,filedata["dateModified"],'n',"",filedata["identifier"][-1]["value"],version,"","",""]
                    x = str(new_snd[0]).replace(' ','')
                    z = str(new_snd[4]).strip('[').strip(']')
                    w = z.strip("'").strip('"')
                    g =  w.split('.')
                    if "." in w:
                        new_snd[0] = x
                        new_snd[3] = g[1]
                        new_snd[4] = w
                        writer.writerow(new_snd)
                        print('added to datalog:')
                        print(x + ' ' + w)
                    else:
                        new_snd[0] = x
                        new_snd[4] = w
                        writer.writerow(new_snd)
                        print('added to datalog:')
                        print(x + ' no slu_id')
                # Hämtar filer till samlingen
                with open(os.path.join(newdir, sndname + '.txt'), 'w') as filnamn_temp:
                    filnamn_temp.write(template.render(filedata))
                with open(os.path.join(newdir, sndname + '.txt'), 'r') as fildata:
                    fil_temp = fildata.read().splitlines()
                    i = 0
                    if os.stat(os.path.join(newdir, sndname + '.txt')).st_size == 0:
                        print('No files, only metadata for '+sndname+'.')
                    for line in fil_temp:
                        lista = line.split(",", 1)
                        i = 1+i
                        r = requests.get(lista[1])
                        if r.status_code == 200:
                            with open(os.path.join(newdir, "" + lista[0]), 'wb') as payload:
                                payload.write(r._content)
                        else:
                            print('No answer')
                if i != 0:
                    print('Files in collection:', i)                    
            # Skriver dublin xml från underlag, snyggar till och ersätter ensamt & med &amp;. 
            # with open(os.path.join(newdir, "" + 'temp_dublin_core.xml'), 'w') as xml:
            dctempl = environment.get_template("dublin_core_get_snds_metadata.jinja")
            with open(os.path.join(newdir, "" + 'temp_dublin_core.xml'), 'w') as xml:                      
                xml.write(dctempl.render(dcitems))

            with open(os.path.join(newdir, "" + 'temp_dublin_core.xml'), 'r') as x, open(os.path.join(newdir, "" + 'dublin_core.xml'), 'w') as y:
                for line in x:
                    if line.rstrip():
                        y.write(line)

            f = open(os.path.join(newdir, "" + 'dublin_core.xml'), 'r')
            filex = f.read()
            f.close()
            if " &" in filex: 
                print('replaces unnallowed &')
                newdata = filex.replace(" &"," &amp;")
                f = open(os.path.join(newdir, "" + 'dublin_core.xml'), 'w')
                f.write(newdata)
                f.close()
                
            # Tar bort underlag från samlingsmapp och .json-ld filer för samlingarna i tempmappen.
            filtemplate = os.path.join(newdir, sndname + '.txt')
            xml_temp = os.path.join(newdir, "" + 'temp_dublin_core.xml')
            os.remove(filtemplate)
            os.remove(xml_temp)
def simp_arch(src, path):
    # Filerna för de hämtade samlingarna kopieras till en undermapp med samlingens titel 
    # som hämtas från underlaget itemtitle.txt. 
    # Samlingen har nu rätt struktur (SimpleArchiveFormat) för att importeras i DSpace.
    for json_meta in glob.glob(src + '/*.json-ld'):
                z = json_meta.split('/')[-1]
                snd, ext = os.path.splitext(z)
                dstdir = os.path.join(path+'/'+snd+'/')
                shutil.move(json_meta, dstdir)
    with open(os.path.join(src+'/new_snds.txt'), 'r') as f:
        collection_list = f.read().splitlines()
    for item in collection_list:
        item1 = item.split(';')[0]
        with open(path+'/'+item1+'/itemtitle.txt', 'r+') as f:
            name = f.read().splitlines()
            print(item1)
            print(name)
            tempfile = os.path.join(path+'/'+item1+'/itemtitle.txt')
            new_name = name[0].replace(' ', '_')
            print(new_name)
        os.remove(tempfile)
        os.mkdir(path+'/'+item1+'/'+new_name)
        new_dir = (path+'/'+item1+'/'+new_name)
        olddir = glob.glob(path+'/'+item1+"/*")
        for item in olddir:
            shutil.move(item, new_dir)
    print('\n')
    print('Simparchs in ' + path)
def remove_temps(path):
    # Tempmapp och kvarvarande filer raderas.
    for temp_noat in glob.glob(os.path.join(path+'/*.json-ld-noat')):
        os.remove(temp_noat)
    for temp_txt in glob.glob(os.path.join(path+'/*.txt')):
        os.remove(temp_txt)
    for temp_xml in glob.glob(os.path.join(path+'/*.xml')):
        os.remove(temp_xml)
    os.removedirs(path)
def sort_datalog():
    # Skriver till vilka samlingar som hämtats i snd_datalog.csv
    with open('snd_datalog.csv', 'r+', encoding='utf8') as d_s:
            writer = csv.writer(d_s, delimiter=";", lineterminator="\n")
            data = pd.read_csv("snd_datalog.csv")
            data.sort_values(data.columns[0], axis=0, inplace=True)
            writer.writerow(data)

if __name__ == '__main__':
    load_xml(sys.argv[1])
    parseXML(sys.argv[1])    
    compare_snds(sys.argv[1])
    get_snds(sys.argv[1])
    load_files(sys.argv[1], sys.argv[2])
    simp_arch(sys.argv[1], sys.argv[2])
    remove_temps(sys.argv[1])
    sort_datalog()
    print('\n'+'Done'+'\n')
    sys.exit()()