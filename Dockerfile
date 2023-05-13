FROM bde2020/spark-worker:3.2.0-hadoop3.2

# RUN apk add --update py-pip
# RUN pip install numpy
RUN apk add -U make automake gfortran gcc g++ subversion python3-dev
