### Sherlock Firebase

#### How to run

We are using Docker to run this in local envoirment to emulate production as a Google Cloud Function. This application requires the C SQLite driver. Google Cloud supports this, sometimes however there are issues with this driver malfunctioning across different versions of Python.

#### With Docker

Build the image:
```
sudo docker build -t sherlock_firebase .
```

Run the image:
```
sudo docker run --net=host sherlock_firebase -oe
```