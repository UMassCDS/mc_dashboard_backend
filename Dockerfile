# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.10-slim

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Setting work directory
WORKDIR /scripts

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    gcc \
    wget \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Double check the right way to do all this change dir stuff 
RUN cd mc_dashboard_pipeline
RUN wget http://www.oslom.org/code/OSLOM2.tar.gz  && tar -xvzf OSLOM2.tar.gz
RUN cd OSLOM2 && chmod 744 compile_all.sh && ./compile_all.sh && cd ../

RUN g++ -std=c++11 -o c_extract_pair extract_pair.cpp
RUN python3 setup.py build_ext --inplace

ENTRYPOINT [ "bash", "./run_code_in_docker.sh"]
