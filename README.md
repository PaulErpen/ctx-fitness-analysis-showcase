# CTx Activity Tracker Project

This project provides code to analyze wearable device data from patients undergoing chemotherapy,
to investigate the connection between indicators of physical activity and treatment response.
All data provided in this showcase is synthetic and does not include any traces of actual patient data.
The repository only serves as a showcase.

The project consists of two parts.
The `interval-parsing` folder contains a data-cleaning and restructuring framework to enable downstream tasks.

The `dashboard` folder implements an interactive visualization of the data.
It can be used to anayze the sparsity of the recorded activity data.
Simpe filters are provided to enable initial testing of inclusion criteria.
A working demo of the dashboard can be found [here](https://ctx-health-tracker-showcase-578841960356.europe-west1.run.app/).
For more information refer to the [project report](./documentation/project-report.pdf) or the [project poster](./documentation/project-poster.pdf).

### Environment Setup

Creating a python environment:

```sh
conda create --name ctx-fitness python=3.9.12
conda activate ctx-fitness
pip install -r docker/requirements.txt
pip install -e ./interval-parsing
pip install -e ./dashboard
```

### Dashboard Setup

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