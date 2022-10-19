import boto3
import re
import time

def getYear(fName):
        yearRegex = re.compile(r'[-_]20\d\d[-_].*.activities.json.gz')
        year = yearRegex.search(fName)
        print('Pattern found: ' + year.group())
        return year.group().strip("-_")[:4]
def main():
	from jproperties import Properties 
	configs = Properties()
	with open('config.properties', 'rb') as config_file:
   		configs.load(config_file)

	destinationSession = boto3.session.Session(profile_name=configs.get("aws.account.destination").data)
	sourceSession = boto3.session.Session(profile_name=configs.get("aws.account.source").data)

	#Then use the session to get the resource
	s3destination = destinationSession.resource('s3')
	s3source = sourceSession.resource('s3')
	source_bucket_name=configs.get("source.bucket.name").data
	source_bucket_prefix =configs.get("source.bucket.prefix").data
	destination_bucket_name=configs.get("destination.bucket.name").data
	destination_bucket_prefix=configs.get("destination.bucket.prefix").data
	sourceBucket = s3source.Bucket(source_bucket_name)
        
	for sourceBucketObject in sourceBucket.objects.filter(Prefix=source_bucket_prefix):
		
    		fileName =  sourceBucketObject.key.split('/')[-1]
    		if fileName:
		  print(fileName)
    		  year=getYear(fileName)
    		  print(year)
    		  source= { 'Bucket' : source_bucket_name,'Key':sourceBucketObject.key}
    		  s3destination.meta.client.copy(source,destination_bucket_name,destination_bucket_prefix+year+'/'+fileName)

from datetime import datetime
startTime = datetime.now()
main()
endTime=datetime.now()
time_diff = endTime - startTime
print("Completed in seconds :",time_diff.seconds)
