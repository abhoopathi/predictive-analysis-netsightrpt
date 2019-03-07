FROM continuumio/anaconda3:5.2.0 
MAINTAINER "Predmac Technologies"

RUN apt-get update && apt-get install -y software-properties-common

#RUN apt-get install -y gnupg 
#RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys  C2518248EEA14886


RUN add-apt-repository ppa:webupd8team/java  
RUN apt-get update
#RUN apt-get install y-ppa-manager
RUN apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections 

RUN apt-get install -y oracle-java8-installer --allow-unauthenticated
    
COPY . /

#RUN /opt/conda/bin/conda install jupyter -y && \

RUN apt-get update && apt-get install -y libgtk2.0-dev 
RUN apt-get update && \
    apt-get install  nano 
    
RUN rm -rf /var/lib/apt/lists/* 

RUN /opt/conda/bin/conda install -c conda-forge py4j=0.10.7
RUN /opt/conda/bin/conda install -c anaconda joblib 
RUN /opt/conda/bin/pip install pyspark
RUN /opt/conda/bin/pip install tqdm 
RUN /opt/conda/bin/pip install sklearn 
RUN /opt/conda/bin/pip install pymysql 
RUN /opt/conda/bin/pip install schedule 
RUN /opt/conda/bin/pip install matplotlib 
RUN /opt/conda/bin/pip install mysql-connector-python 
RUN /opt/conda/bin/conda install -c conda-forge fbprophet 

RUN chmod +x /script.sh   
CMD ["/script.sh"]
