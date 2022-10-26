FROM ubuntu:focal
MAINTAINER Oleg Smirnov "osmirnov@gmail.com"

ARG DEBIAN_FRONTEND=noninteractive
RUN apt -y update && \
    apt -y upgrade && \
        apt install -y \
            python3-pip \
            python3-minimal \
            python-is-python3 \
            git && \
    apt clean


RUN python -m pip install -U pip setuptools wheel dask-ms[s3,zarr,arrow]

ADD src/QuartiCal src/QuartiCal
RUN ls src
RUN ls src/QuartiCal
RUN python -m pip install -U src/QuartiCal 
RUN python -m pip cache purge
