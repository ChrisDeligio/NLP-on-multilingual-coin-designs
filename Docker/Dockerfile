FROM python:3.7.13-buster
RUN pip install --upgrade pip
RUN mkdir /code
WORKDIR /code

RUN pip install -U spacy==2.3.7
RUN python -m spacy download en_core_web_sm
RUN pip install pandas 
RUN pip install fastapi[all]
RUN pip3 install rdflib
RUN pip3 install mysql
RUN pip install mysql-connector-python
RUN pip install SQLAlchemy
RUN pip3 install -U scikit-learn
RUN pip install jupyterlab

COPY . .

EXPOSE 80
CMD ["python3"]