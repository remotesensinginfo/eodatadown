# Dockerfile with EODataDown + SNAP
FROM petebunting/au-eoed:20200421

LABEL authors="Dan Clewley"
LABEL maintainer="dac@pml.ac.uk"

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/miniconda/bin:$PATH
ENV DEBIAN_FRONTEND=noninteractive

# Install packages needed
RUN apt-get update --fix-missing && \
    apt-get install -y apt-utils wget bzip2 apt-transport-https ca-certificates gcc gnupg curl git binutils vim make unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# pip install psycopg2, do this rather than via conda-forge to avoid any conflicts
RUN pip install psycopg2

# Install gsutil (using instructions from https://cloud.google.com/sdk/docs/install#deb)
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y
RUN pip install --upgrade protobuf && pip install google-cloud google-cloud-storage google-cloud-bigquery

# Install SNAP
RUN  wget --quiet https://download.esa.int/step/snap/8.0/installers/esa-snap_sentinel_unix_8_0.sh && \
    bash ./esa-snap_sentinel_unix_8_0.sh -q && \
    rm esa-snap_sentinel_unix_8_0.sh

# Install LivingWales branch of EODataDown
RUN mkdir -p /opt/eodatadown . && \
    cd /opt/eodatadown && \
    git clone https://github.com/remotesensinginfo/eodatadown.git . && \
	git checkout -b living_wales_ard_changes origin/living_wales_ard_changes && \
    python ./setup.py install && \
    cd /opt && \
    rm -Rf ./eodatadown && \
    sync

# Install latest version of pb_process_tools
RUN mkdir -p /opt/pb_process_tools && \
    cd /opt/pb_process_tools && \
    git clone https://github.com/remotesensinginfo/pb_process_tools.git . && \
    python ./setup.py install && \
    cd /opt && \
    rm -Rf ./pb_process_tools && \
    sync

# Install latest version of PyroSAR. Install from GitHub rather than via conda-forge to reduce confilcts
RUN mkdir -p /opt/pyrosar && \
    cd /opt/pyrosar && \
    git clone https://github.com/johntruckenbrodt/pyroSAR.git . && \
    python -m pip install . && \
    cd /opt && \
    rm -Rf ./pyrosar && \
    sync

# Tidy up
RUN conda clean --all -y

WORKDIR /home

# Add Tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD [ "/bin/bash" ]

