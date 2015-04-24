'''
Created on 24 Apr 2015

@author: setup
'''
import sys
import re 
import json


def convert2ISOformat(matches):
    '''Do some things.
    :param verbose: Be verbose (give additional messages).
    '''
    print matches
    return  matches[4]+"-"+mon2num[matches[1]]+"-"+matches[2]+"T"+matches[3]+".000Z";
    #raise NotImplementedError

def write_to_file(job_dictionary):
    '''Do some things.
    :param verbose: Be verbose (give additional messages).
    '''
    json.dumps(job_dictionary)
    with open('data.txt', 'w') as outfile:
        print '{"index":{"_index":"monit","_type":"';
        print job_dictionary['search_type']+'"}}';
        print "\n";
        json.dump(job_dictionary, outfile)

if __name__ == '__main__':
    job_dictionary={} 
    fastest_time=1000
    fastest_worker=None
    slowest_worker=None
    slowest_time=0
    no_registered_workers=0
    sum_runtimes=0
    
    seq_db_mapping = {
                      '1' :  "uniprotkb",
                      '2' :  "pfamseq",        
                      '3' :  "nr", 
                      '4' :  "refseq",         
                      '5' :  "rp15",
                      '6' :  "rp35",           
                      '7' :  "rp55",          
                      '8' :  "rp75",           
                      '9' :  "uniprotrefprot", 
                      '10' : "swissprot",      
                      '11' : "env_nr",         
                      '12' : "unimes",         
                      '13' : "pdb"
                      }
    hmm_db_mapping = {
                      '1' :  "gene3d",
                      '2' :  "pfam",        
                      '3' :  "superfamily", 
                      '4' :  "tigrfam",         
                      '5' :  "pirsf"
    }
    
    ''' month 2 number mapping '''
    mon2num = {
               'Jan': '01','Feb': '02','Mar': '03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'
               }
    ''' regex used '''
    date_regex = re.compile(r'(\w+)\s*(\w+)\s+(\d+)\s+(\d+:\d+:\d+)\s+(\d+)', re.IGNORECASE)
    queue_regex = re.compile(r'Queuing \w+ (.*) from (\d+.\d+.\d+.\d+) \(\d+\)', re.IGNORECASE)
    hits_regex = re.compile(r'Hits:(\d+)  reported:(\d+)  included:(\d+)', re.IGNORECASE)
    targetdb_regex = re.compile(r'hmmpgmd.*\--(hmm|seq)db\s(\d+)', re.IGNORECASE)
    worker_regex = re.compile(r'WORKER (10.7.50.\d+) COMPLETED: (\d+\.?\d*) sec received (\d+) bytes', re.IGNORECASE)
    closing_regex = re.compile(r'Closing', re.IGNORECASE)
    
    with open("../data/small.hmmpgmd.log", "r") as txt:
        for line in txt:
            ''' match date '''
            if(re.match(date_regex,line)):
                matches = re.findall(date_regex,line)
                date_format = convert2ISOformat(matches[0])
                print date_format
                next
            ''' queuing the sequence'''
            if(re.match(queue_regex, line)):
                matches = re.findall(queue_regex,line)
                if(matches and matches[0]):
                    job_dictionary['query_id'] = matches[0][0]  

            ''' hits reported'''
            if(re.match(hits_regex, line)):
                matches = re.findall(hits_regex,line)
                if(matches and matches[0]):
                    job_dictionary['hits'] = matches[0][0]  
                    job_dictionary['reported'] = matches[0][1]  
                    job_dictionary['included'] = matches[0][2]  

            ''' target db'''
            if(re.match(targetdb_regex, line)):
                matches = re.findall(targetdb_regex,line)
                if(matches and matches[0]):
                    job_dictionary['search_type'] = matches[0][0] 
                    if(matches[0][0] == 'hmm'):                   
                        job_dictionary['target_db'] = hmm_db_mapping[matches[0][1]]  
                    else:
                        job_dictionary['target_db'] = seq_db_mapping[matches[0][1]] 
         
            ''' worker '''
            if(re.match(worker_regex, line)):    
                matches = re.findall(worker_regex,line)
                if(matches):
                    worker_id = matches[0][0]
                    runtime = float(matches[0][1])
                    no_registered_workers += 1
                    sum_runtimes += runtime;
                    job_dictionary[runtime] = runtime
                    if(fastest_time ==1000 or runtime <= fastest_time ):
                                (fastest_worker,fastest_time) = (worker_id,runtime)
                    if(slowest_time ==1000 or runtime > slowest_time ):
                                (slowest_worker,slowest_time) = (worker_id,runtime)
                                
            ''' worker '''
            if(re.match(closing_regex, line)):  
                    job_dictionary['slowest_worker'] = slowest_worker
                    job_dictionary['slowest_time'] = float(slowest_time)
                    job_dictionary['fastest_worker'] = fastest_worker
                    job_dictionary['fastest_time'] = float(fastest_time)
                    job_dictionary['time_diff'] = slowest_worker
                    job_dictionary['perc_diff'] = slowest_worker
                    job_dictionary['speed_tradeoff'] = slowest_worker
                    job_dictionary['mean_time'] = sum_runtimes/no_registered_workers if no_registered_workers>0 else null
                    
                    write_to_file(job_dictionary)