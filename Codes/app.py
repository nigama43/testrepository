import shlex
import subprocess
import json
import ast
from ast import literal_eval
import time
import os
from azure.eventhub import EventHubProducerClient, EventData

def run(response):
    CONNECTION_STR='Endpoint=sb://sqlonpremcdc.servicebus.windows.net/;SharedAccessKeyName=SharedAccessSend;SharedAccessKey=K0PoaxZq55MisUwr2nwrQkPXYDlQFYa+HVceVcm54hI='
    EVENTHUB_NAME = 'demostatus'


    start_time = time.time()

    producer = EventHubProducerClient.from_connection_string(
         conn_str=CONNECTION_STR,
         eventhub_name=EVENTHUB_NAME
)
    with producer:
       event_data_batch = producer.create_batch()
       event_data_batch.add(EventData(f'{response}'))
    
       producer.send_batch(event_data_batch)

           
def main():
    conn=["sql-server-connection_2","sql-server-connection_5","sql-server-connection_1"]
    for connector in conn:
        cmd = "curl -s http://localhost:8083/connectors/"+connector+"/status"
        args = shlex.split(cmd)

        process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        data = literal_eval(stdout.decode('utf8'))
        s = json.dumps(data, indent=4, sort_keys=True)
        resp = json.loads(s)
        v1=list(resp.keys())    
        if (v1[0]) == ('error_code'):
           run(resp)
           print('errorcode')
                       
        else:
            v2=resp['tasks'][0]['state']
            if v2 == ('FAILED'):
               run(resp)
               print('failed') 

if __name__ == "__main__":
    main()               
          
    
    
