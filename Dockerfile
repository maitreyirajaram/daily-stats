FROM python:2.7
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN echo "Y" | ACCEPT_EULA=Y apt-get install msodbcsql17
RUN apt-get install unixodbc-dev
ADD requirements.txt .
RUN pip install -r requirements.txt
ADD connect.py .
CMD ["python", "connect.py", "name of secret"]
