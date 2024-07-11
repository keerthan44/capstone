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
## Step 3: Run the python app.py
1.    python app.py
2. Open link http://127.0.0.1:3000
3. Upload the trace_data. Sample one is give in ./data
## Step 4: Create and deploy the containers
    cd containers
    python create.py
## Step 5: To see logs 
    docker logs -f logging_capstone
## Step 6: To destroy all containers
    cd containers
    python destroy.py
