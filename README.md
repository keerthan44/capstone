# **Python Static Application**
All steps from root directory
## Step 1: Build the flask worker container
    cd containers/app/
    docker build -t flask-contact-container .
## Step 2: Build the logging container
```shell
cd containers/logging/
docker build -t logging_capstone  .
```
## Step 3: 
## Option 1: Use Alibaba Data -- Sample Data 30 Microservices
1. cd alibaba
2. python app.py
3. Open link http://127.0.0.1:3000
4. Upload the trace_data. Sample one is give in ./alibaba/data
## Option 2: Use DeathStarBench Data -- Sample Data 3 Microservices
1.  cd deathstarbench
2.  python extractDataV2.py.  -- this will create traces.json  
3.  Choose the file. Sample one is give in ./deathstarbench/data.
4.  python processData.py
## Step 4: Create and deploy the containers
    cd containers
    python create.py
## Step 5: To see logs 
    docker logs -f logging_capstone
## Step 6: To destroy all containers
    cd containers
    python destroy.py
