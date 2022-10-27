import os
import boto3
from jproperties import Properties


def partition(list, size):
    for i in range(0, len(list), size):
        yield list[i:i + size]


def sendBatch(batch):
    response = sqs_client.send_message_batch(QueueUrl=configs.get("aws.sqs.queue.uri").data,
                                        Entries=batch)
     # Print out any failures
    
    if(response.get('Failure') is not None):
            print('Failure :'+str(len(response.get('Failure'))))
            print(response.get('Failure'))

sent_count=0

try:

    #Load configuration file
    configs = Properties()
    with open("config/config.properties", "rb") as config_file:
        configs.load(config_file)

    #Establish SQS AWS Connection
    aws_session = boto3.session.Session(profile_name=configs.get("aws.account.profile").data)
    sqs_client = aws_session.client('sqs')

    #Read all lines from files in the source directory
    all_lines = []
    with os.scandir(configs.get("source.file.dir").data) as entries:
        for entry in entries:
            if entry.is_file():
                open_file = open(entry.path, 'r')
                lines = open_file.readlines()
                for line in lines:
                    all_lines.append(line.strip())

    #Convert lines into entries to send as batches to SQS
    entries = []
    count = 0
    for message in all_lines:
        count = count + 1
        entry =  {
            'Id': 'id%s' % str(count),
            'MessageBody': str(message)
            }
        entries.append(entry)
    
    #Convert entries into batches of size n      
    all_batch_messages = list(partition(entries, int(configs.get("batch.size").data)))

    
                                
    #Send messages to Queue
    for batch in all_batch_messages:
        sendBatch(batch)
        sent_count = sent_count + len(batch)
        print("Sent Messages:"+ str(sent_count))
        
except KeyboardInterrupt:
       print("Program terminated manually!")
       raise SystemExit
except Exception as e:
    print(e)
