# Health tracker project

This project serves to analyze smartwatch/wristband data from patients undergoing chemotherapy.
The data includes variables like steps taken and heart rate.

Creating a python environment:

```sh
conda create --name ctx-fitness python=3.9.12
conda activate ctx-fitness
pip install -r docker/requirements.txt
pip install -e ./interval-parsing
pip install -e ./dashboard
```

To build Dockerfile execute the following from the root directory of this project:

```sh
docker build -f docker/Dockerfile -t "ctx-fitness" .
```

To run the docker image use:
```sh
docker run -p 8080:8080 -e PORT=8080 -e PROFILES=local ctx-fitness
```

To deplopy use:

```sh
gcloud builds submit --region=europe-west1
```