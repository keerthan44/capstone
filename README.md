# **Python Static Application**
All steps from root directory
## Prerequisites
### Step 1: Install dependencies
1. Install Python dependencies 
```shell 
pip install -r requirements.txt
```
2. Kubernetes(kubectl)
```shell
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
sudo chmod 644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg
```
if these steps didn't work for you, then go to [Kubernetes Official Page](https://kubernetes.io/docs/tasks/tools/ "Kubernetes Official Page")
3. Minikube
```shell
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
```
if these steps didn't work for you, then go to [Minikube Official Page](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+downloadhttp:// "Minikube Official Page")
```shell
minikube start --driver=docker
```
4. Add ./containers/.env
```shell
DOCKER_USERNAME=<your_dockerhub_username>, # not used as of now
KUBERNETES_NAMESPACE=<your_k8s_namespace>, # this is used to create a namespace for all the microservices
#NODEPORT <VALUE HAS TO BE BETWEEN 30000-32767>
KAFKA_EXTERNAL_GATEWAY_NODEPORT=32092 # for this variable you can just leave the value as it is
```
### Step 2: Minikube Tunnel
Open a new terminall and make sure the code below is running always. 
```shell
minikube tunnel
```
## Step 1: Build the flask worker container and push to minikube
```shell
eval $(minikube docker-env)
cd containers/app/
docker build -t flask-contact-container .
```
## Step 2: Build the logging container and push to minikube
```shell
eval $(minikube docker-env)
cd containers/logging/
docker build -t logging_capstone  .
```
## Step 3: Build the Kafka Gateway Container and push to minikube
```shell
eval $(minikube docker-env)
cd containers/kafka_external_gateway/
docker build -t kafka-external-gateway  .
```
## Step 4: 
### Option 1: Use Alibaba Data -- Sample Data 30 Microservices
1. cd alibaba
2. python app.py
3. Open link http://127.0.0.1:3000
4. Upload the trace_data. Sample one is give in ./alibaba/data
### Option 2: Use DeathStarBench Data -- Sample Data 3 Microservices
1.  cd deathstarbench
2.  python extractDataV2.py.  -- this will create traces.json  
3.  Choose the file. Sample one is give in ./deathstarbench/data.
4.  python processData.py
## Step 5: Create and deploy the containers
```shell
cd containers
python create.py
```
## Step 6: To see logs 
```shell
POD_NAME=$(kubectl get pods -n static-application | grep logging | awk '{print $1}')  # -n is the namespace will vary based on .env
kubectl logs $POD_NAME -n static-application
```
## Step 7: To destroy all containers
```shell
cd containers
python destroy.py
```
