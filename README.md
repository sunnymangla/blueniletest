# blueniletest

build local :
----------------

1. mvn install

Run code into local :
----------------------------

1. download google account credential file from your GCP account.
2. Save it on local system.
3. Set Environment variable GOOGLE_APPLICATION_CREDENTIALS with file path
4. mvn run
5. local server will stand up on 8080 port.

CURL to test:
curl --location --request POST 'http://localhost:8080?num=100,2,3'


Note : Refer cloud functions in GCP section to upload code via zip or manual copy in GCP console.

1. Function BluenileRequestHandler - accept http request as per above curl.

2. Function StoreNumbersSubscriber - gets trigerred through a topic. need to subscribe function to topic.
