FROM stackbrew/ubuntu:14.04

MAINTAINER Bruno Renié <bruno@renie.fr>

VOLUME /srv/graphite

RUN apt-get update
RUN apt-get upgrade -y

RUN apt-get install -y language-pack-en
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN locale-gen en_US.UTF-8
RUN dpkg-reconfigure locales

RUN apt-get install -y build-essential python-dev libffi-dev libcairo2-dev python-pip git 

RUN git clone https://github.com/dkulikovsky/graphite-api.git

RUN cd graphite-api && python setup.py install

RUN pip install gunicorn Flask-Cache

ONBUILD ADD conf/graphite-api.yaml /etc/graphite-api.yaml
ONBUILD ADD conf/gunicorn_logging.conf /etc/gunicorn_logging.conf
ONBUILD RUN chmod 0644 /etc/graphite-api.yaml
ONBUILD RUN chmod 0644 /etc/gunicorn_logging.conf

EXPOSE 8000

CMD gunicorn -b 0.0.0.0:8000 -w 4 --log-level debug graphite_api.app:app --log-config /etc/gunicorn_logging.conf
